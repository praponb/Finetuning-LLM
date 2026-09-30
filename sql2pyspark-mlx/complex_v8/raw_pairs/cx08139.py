from pyspark.sql import functions as F

filtered_skus = (
    df_skus
    .filter(F.col("unit_cost") < 239.3)
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "labels",
        F.round(F.col("unit_cost") / F.nullif(F.col("weight_kg"), F.lit(0)), 3).alias("unit_cost_per_weight_kg"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.min("unit_price").alias("min_move_unit_price"))
)

profiled = (
    filtered_skus.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select("s.*", "st.min_move_unit_price")
)

result = (
    profiled
    .select("unit_cost", "id")
    .distinct()
    .orderBy(F.col("id").desc())
)
