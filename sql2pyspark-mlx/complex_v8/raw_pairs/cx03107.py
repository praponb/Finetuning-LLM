from pyspark.sql import functions as F

sku_side = (
    df_skus
    .select(
        F.col("id").alias("ref_id"),
        F.col("sku_code").alias("name"),
        F.lit("sku").alias("src"),
    )
)

sku_rows = (
    df_skus
    .filter(F.col("unit_cost") > 134.3)
    .select("id", "sku_code", F.lit("sku_2").alias("src"))
)

result = (
    sku_side
    .union(sku_rows)
    .distinct()
)
