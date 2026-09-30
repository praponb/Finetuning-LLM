from pyspark.sql import functions as F

recent = (
    df_skus
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.when(F.col("unit_cost") >= 306.4, "HIGH").when(F.col("unit_cost") >= 108.5, "MEDIUM").otherwise("LOW").alias("unit_cost_tier"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count("*").alias("move_count_2"),
        F.count("*").alias("move_count_3"),
    )
)

with_counts = (
    recent.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
        F.coalesce(F.col("st.move_count_3"), F.lit(0)).alias("move_count_3"),
    )
)

result = (
    with_counts
    .select("id", "unit_cost", "weight_kg")
    .unpivot(
        ids=["id"],
        values=["unit_cost", "weight_kg"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "measure")
)
