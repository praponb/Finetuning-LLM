from pyspark.sql import functions as F

recent = (
    df_skus
    .filter(F.col("weight_kg") > 6.5)
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "labels",
        "is_active",
    )
)

sku_side = recent.select(F.lit("sku").alias("record_type"), "category", "id")

sku_side_2 = (
    df_skus
    .filter(F.upper(F.trim(F.col("sku_code"))).like("E%"))
    .select(F.lit("sku_2").alias("record_type"), "sku_code", "id")
)

all_events = sku_side.union(sku_side_2)

result = (
    all_events
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("record_type")
)
