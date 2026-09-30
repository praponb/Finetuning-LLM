from pyspark.sql import functions as F

cleaned = (
    df_warehouses
    .filter((F.col("opened_on") >= F.lit("2024-11-25").cast("date")) & (F.col("city").isin("Singapore", "Kuala Lumpur")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("unit_price").alias("min_move_unit_price"),
        F.count("*").alias("move_count"),
    )
)

with_stats = (
    cleaned.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.min_move_unit_price",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

with_activity = (
    with_stats
    .crossJoin(with_stats.agg(F.min("move_count").alias("overall_min_move_count")))
    .filter(F.col("move_count") > F.col("overall_min_move_count"))
    .drop("overall_min_move_count")
)

result = (
    with_activity
    .cube("move_count", "city")
    .agg(F.countDistinct("city").alias("distinct_citys"))
    .orderBy(F.col("move_count").asc_nulls_last(), F.col("city").asc_nulls_last())
)
