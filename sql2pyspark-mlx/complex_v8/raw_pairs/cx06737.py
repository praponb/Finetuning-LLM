from pyspark.sql import functions as F

scoped = (
    df_warehouses
    .filter((F.col("opened_on") >= F.lit("2024-04-13").cast("date")) & (F.lower(F.col("wh_code")).like("%ku%")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("move_type") == "INBOUND", 1)).alias("move_move_type_count"),
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
    )
)

with_stats = (
    scoped.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
        "st.avg_move_unit_price",
    )
)

result = (
    with_stats
    .cube("city", "city_or_unknown", "move_move_type_count")
    .agg(
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("wh_code"))).like("K%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_wh_code_like_k"),
    )
)
