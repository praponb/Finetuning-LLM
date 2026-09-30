from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_move_unit_price = Window.orderBy("total_move_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_total_move_unit_price_2 = Window.orderBy("total_move_unit_price", "id")
w_opened_on_month_by_total_mov = Window.partitionBy(F.date_trunc("month", "opened_on")).orderBy("total_move_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_total_move_unit_price_3 = Window.orderBy("total_move_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_warehouses
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.when(F.col("capacity") >= 11506, "HIGH").when(F.col("capacity") >= 4912, "MEDIUM").otherwise("LOW").alias("capacity_tier"),
        F.when(F.col("city").isin("Bangkok", "Singapore"), "group_a").when(F.col("city") == "Kuala Lumpur", "group_b").otherwise("other").alias("city_group"),
        F.concat_ws(": ", F.col("wh_code"), F.col("city")).alias("wh_code_city_label"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.min("qty").alias("min_move_qty"),
        F.sum("unit_price").alias("total_move_unit_price"),
    )
)

profiled = (
    base.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.min_move_qty",
        "st.total_move_unit_price",
    )
)

with_activity = (
    profiled.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left_semi")
)

screened = (
    with_activity
    .crossJoin(with_activity.agg(F.min("min_move_qty").alias("overall_min_min_move_qty")))
    .filter(F.col("min_move_qty") > F.col("overall_min_min_move_qty"))
    .drop("overall_min_min_move_qty")
)

with_activity_2 = (
    screened.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.to_date(F.col("st.moved_at")) < F.lit("2025-02-24").cast("date")) & (F.col("st.sku_id").isNotNull()), "left_anti")
    .filter(F.year(F.col("opened_on")) == 2025)
)

with_windows = (
    with_activity_2
    .select(
        "*",
        F.sum("move_count").over(w_all_by_total_move_unit_price).alias("running_move_count"),
        F.lag("capacity", 3).over(w_all_by_total_move_unit_price_2).alias("prev3_capacity"),
        F.sum("capacity").over(w_opened_on_month_by_total_mov).alias("running_capacity"),
        F.nth_value("total_move_unit_price", 2).over(w_all_by_total_move_unit_price_3).alias("second_total_move_unit_price"),
        F.min("min_move_qty").over(w_opened_on_month_by_total_mov).alias("min_min_move_qty_so_far"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        "wh_code_city_label",
        "move_count",
        "total_move_unit_price",
        "running_move_count",
        "prev3_capacity",
        "running_capacity",
        "second_total_move_unit_price",
        (F.col("move_count") + F.col("capacity")).alias("move_count_plus_capacity"),
    )
)

result = (
    enriched
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        "move_count",
        "total_move_unit_price",
        "running_move_count",
        "prev3_capacity",
        "move_count_plus_capacity",
        F.dayofweek(F.col("opened_on")).alias("opened_on_dayofweek"),
    )
    .orderBy(F.col("capacity").desc(), "city")
)
