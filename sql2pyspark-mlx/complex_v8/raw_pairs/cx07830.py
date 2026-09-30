from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_on_full = Window.orderBy("opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_on_month_month_city = Window.partitionBy(F.date_trunc("month", "opened_on_month"), "city")
w_all_by_opened_on_moving_2 = Window.orderBy("opened_on", "id").rowsBetween(-2, Window.currentRow)

prepared = (
    df_warehouses
    .filter(F.col("wh_code").isNotNull())
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.length(F.trim(F.col("city"))).alias("city_len"),
        F.date_trunc("month", F.col("opened_on")).alias("opened_on_month"),
        F.concat_ws("-", F.col("city"), F.col("wh_code")).alias("city_wh_code_label"),
    )
)

windowed = (
    prepared
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        "city_len",
        "opened_on_month",
        "city_wh_code_label",
        F.first("opened_on").over(w_all_by_opened_on_full).alias("first_opened_on"),
        F.round(F.col("capacity") - F.avg("capacity").over(w_opened_on_month_month_city), 2).alias("capacity_vs_opened_on_month_month_city_avg"),
        F.sum("capacity").over(w_opened_on_month_month_city).alias("opened_on_month_month_city_sum_capacity"),
        F.count(F.lit(1)).over(w_opened_on_month_month_city).alias("rows_in_opened_on_month_month_city"),
        F.sum("city_len").over(w_all_by_opened_on_moving_2).alias("city_len_sum_3"),
    )
)

city_stats = (
    windowed
    .groupBy("city")
    .agg(F.percentile_approx("opened_on_month_month_city_sum_capacity", 0.5).alias("median_opened_on_month_month_city_sum_capacity"))
)

outliers = (
    windowed.alias("s")
    .join(city_stats.alias("g"), F.col("g.city") == F.col("s.city"), "inner")
    .filter(F.col("s.opened_on_month_month_city_sum_capacity") >= F.col("g.median_opened_on_month_month_city_sum_capacity"))
    .select(
        "s.*",
        "g.median_opened_on_month_month_city_sum_capacity",
        F.round(F.col("s.opened_on_month_month_city_sum_capacity") - F.col("g.median_opened_on_month_month_city_sum_capacity"), 2).alias("opened_on_month_month_city_sum_capacity_minus_me"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.max("qty").alias("max_move_qty"),
    )
)

summarized_rows = (
    outliers.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select("s.*", "st.last_moved_at", "st.max_move_qty")
)

result = (
    summarized_rows
    .select(
        "id",
        "city_len",
        "rows_in_opened_on_month_month_city",
        "max_move_qty",
    )
    .unpivot(
        ids=["id"],
        values=["city_len", "rows_in_opened_on_month_month_city", "max_move_qty"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
