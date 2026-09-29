from pyspark.sql import functions as F

base_warehouses = (
    df_warehouses
    .filter(F.col("city").isin("Singapore", "Kuala Lumpur"))
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.concat(F.col("city"), F.lit("#"), F.col("capacity").cast("string")).alias("city_ref"),
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
    )
)

filtered = (
    base_warehouses.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.move_type") == "TRANSFER"), "left_semi")
    .filter(F.col("capacity") >= 2614)
)

scored = filtered.withColumn("wh_code_len", F.length(F.trim(F.col("wh_code"))))

with_children_moves = (
    scored.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.capacity",
        "s.opened_on",
        "s.city_ref",
        "s.wh_code_len",
        F.col("st.id").alias("move_id"),
        F.col("st.sku_id").alias("move_sku_id"),
        "st.move_type",
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

result = (
    with_children_moves
    .groupBy("opened_on", "move_id")
    .pivot("move_type", ["ADJUST", "OUTBOUND", "TRANSFER"])
    .agg(F.avg("capacity").alias("avg_v"), F.count("capacity").alias("n"))
    .orderBy("opened_on", "move_id")
)
