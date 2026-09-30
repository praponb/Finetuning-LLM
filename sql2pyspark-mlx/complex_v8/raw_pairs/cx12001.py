from pyspark.sql import functions as F

sku_side = (
    df_skus
    .filter((~F.col("category").isin("Electronics", "Beauty")) & (F.array_contains(F.col("labels"), "bulky")))
    .select("sku_code", "weight_kg")
)

sku_rows = df_skus.select("sku_code", "weight_kg")

stacked = sku_side.union(sku_rows)

result = (
    stacked
    .groupBy("sku_code")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("weight_kg").alias("total_weight_kg"),
    )
    .orderBy("sku_code")
)
