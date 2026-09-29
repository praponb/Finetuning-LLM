from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_group_opened_on_dayofwe = Window.partitionBy("city_group", "opened_on_dayofweek")
w_all_by_capacity_running = Window.orderBy("capacity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_capacity = Window.orderBy("capacity", "id")

scoped = (
    df_warehouses
    .filter(F.col("city") == "Kuala Lumpur")
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        F.dayofweek(F.col("opened_on")).alias("opened_on_dayofweek"),
        F.when(F.col("capacity") >= 11506, "A").when(F.col("capacity") >= 4912, "B").otherwise("C").alias("capacity_tier"),
        F.when(F.col("city").isin("Singapore", "Kuala Lumpur"), "group_a").when(F.col("city") == "Bangkok", "group_b").otherwise("other").alias("city_group"),
    )
)

city_stats = (
    scoped
    .groupBy("city")
    .agg(F.percentile_approx("opened_on_dayofweek", 0.5).alias("median_opened_on_dayofweek"))
)

compared = (
    scoped.alias("s")
    .join(city_stats.alias("g"), F.col("g.city") == F.col("s.city"), "inner")
    .filter(F.col("s.opened_on_dayofweek") >= F.col("g.median_opened_on_dayofweek"))
    .select(
        "s.*",
        "g.median_opened_on_dayofweek",
        F.round(F.col("s.opened_on_dayofweek") - F.col("g.median_opened_on_dayofweek"), 2).alias("opened_on_dayofweek_minus_median"),
    )
)

sequenced = (
    compared
    .select(
        "*",
        F.count(F.lit(1)).over(w_city_group_opened_on_dayofwe).alias("rows_in_city_group_opened_on_dayofweek"),
        F.round(F.col("opened_on_dayofweek") - F.avg("opened_on_dayofweek").over(w_city_group_opened_on_dayofwe), 2).alias("opened_on_dayofweek_vs_city_group_opened_on_dayo"),
        F.first("opened_on_dayofweek", ignorenulls=True).over(w_all_by_capacity_running).alias("first_known_opened_on_dayofweek"),
        F.round(F.col("capacity") - F.avg("capacity").over(w_city_group_opened_on_dayofwe), 2).alias("capacity_vs_city_group_opened_on_dayofweek_avg"),
        F.max("opened_on_dayofweek").over(w_city_group_opened_on_dayofwe).alias("city_group_opened_on_dayofweek_max_opened_on_day"),
        F.ntile(3).over(w_all_by_capacity).alias("capacity_tercile"),
    )
)

filtered = (
    sequenced.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left_semi")
)

with_children = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.median_opened_on_dayofweek",
        "s.first_known_opened_on_dayofweek",
        "s.capacity_vs_city_group_opened_on_dayofweek_avg",
        "s.city_group_opened_on_dayofweek_max_opened_on_day",
        F.col("st.id").alias("move_id"),
        "st.move_type",
        F.col("st.operator").alias("move_operator"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty"),
    )
)

filtered_2 = (
    with_children.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.unit_price").between(95.8, 412.4)) & (F.col("st.move_type").isin("TRANSFER", "OUTBOUND")), "left_semi")
)

result = (
    filtered_2
    .filter(F.col("capacity_vs_city_group_opened_on_dayofweek_avg") <= 12000.0)
    .select(
        "id",
        "median_opened_on_dayofweek",
        "first_known_opened_on_dayofweek",
        "capacity_vs_city_group_opened_on_dayofweek_avg",
        "city_group_opened_on_dayofweek_max_opened_on_day",
        "move_id",
        "move_type",
        "move_operator",
        "move_moved_at",
        "move_qty",
        F.abs(F.col("move_qty") - 216).alias("move_qty_distance"),
    )
)
