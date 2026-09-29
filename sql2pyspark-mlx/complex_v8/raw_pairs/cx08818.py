from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_on_plus_1m_mov = Window.orderBy(F.col("opened_on_plus_1m").desc(), "id").rowsBetween(-6, Window.currentRow)
w_opened_on_plus_1m_month = Window.partitionBy(F.date_trunc("month", "opened_on_plus_1m"))

scoped = (
    df_warehouses
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.concat(F.col("wh_code"), F.lit("#"), F.col("id").cast("string")).alias("wh_code_ref"),
        F.date_format(F.col("opened_on"), "yyyy-MM").alias("opened_on_label"),
        F.substring(F.trim(F.col("wh_code")), 1, 4).alias("wh_code_prefix"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("unit_price").alias("min_move_unit_price"),
        F.sum("qty").alias("total_move_qty"),
    )
)

profiled = (
    scoped.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select("s.*", "st.min_move_unit_price", "st.total_move_qty")
)

annotated = (
    profiled
    .select(
        "id",
        "capacity",
        "opened_on",
        "opened_on_label",
        "wh_code_prefix",
        "min_move_unit_price",
        "total_move_qty",
        F.add_months(F.col("opened_on"), 1).alias("opened_on_plus_1m"),
    )
)

with_trends = (
    annotated
    .select(
        "id",
        "capacity",
        "opened_on",
        "opened_on_label",
        "min_move_unit_price",
        "total_move_qty",
        "opened_on_plus_1m",
        F.sum("total_move_qty").over(w_all_by_opened_on_plus_1m_mov).alias("total_move_qty_sum_7"),
        F.round(F.avg("total_move_qty").over(w_opened_on_plus_1m_month), 2).alias("opened_on_plus_1m_month_avg_total_move_qty"),
    )
)

labeled = (
    with_trends
    .filter(F.col("opened_on") >= F.lit("2024-11-25").cast("date"))
    .withColumn("opened_on_label_prefix", F.substring(F.trim(F.col("opened_on_label")), 1, 4))
)

tagged = (
    labeled
    .select(
        "id",
        "capacity",
        "opened_on",
        "opened_on_label",
        "min_move_unit_price",
        "total_move_qty",
        "total_move_qty_sum_7",
        "opened_on_plus_1m_month_avg_total_move_qty",
        "opened_on_label_prefix",
        F.abs(F.col("min_move_unit_price") - 150.7).alias("min_move_unit_price_distance"),
    )
)

grouped = (
    tagged
    .groupBy(
        F.when(F.col("total_move_qty_sum_7") >= 7200, "A").when(F.col("total_move_qty_sum_7") >= 2701, "B").otherwise("C").alias("total_move_qty_sum_7_tier"),
        F.year("opened_on").alias("opened_on_year"),
    )
    .agg(
        F.max("opened_on").alias("last_opened_on"),
        F.percentile_approx("capacity", 0.5).alias("median_capacity"),
        F.count("*").alias("n_rows"),
        F.round(F.sum("min_move_unit_price_distance") / F.nullif(F.sum("opened_on_plus_1m_month_avg_total_move_qty"), F.lit(0)), 3).alias("min_move_unit_price_distance_to_opened_on_plus_1"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .filter(F.col("last_opened_on") < F.lit("2025-07-03").cast("date"))
    .select(
        "total_move_qty_sum_7_tier",
        "last_opened_on",
        "median_capacity",
        "n_rows",
        "min_move_unit_price_distance_to_opened_on_plus_1",
        "distinct_ids",
        F.date_format(F.col("last_opened_on"), "yyyy-MM").alias("last_opened_on_label"),
    )
    .orderBy(F.col("min_move_unit_price_distance_to_opened_on_plus_1").desc())
)
