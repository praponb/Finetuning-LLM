from pyspark.sql import functions as F

sku_side = (
    df_skus
    .select(
        F.col("id").alias("key_id"),
        F.col("unit_cost").alias("metric"),
        F.lit("sku").alias("source"),
    )
)

sku_rows = (
    df_skus
    .select(
        F.col("id").alias("key_id"),
        F.col("weight_kg").alias("metric"),
        F.lit("sku_2").alias("source"),
    )
)

stacked = sku_side.unionByName(sku_rows)

result = (
    stacked
    .groupBy("source")
    .agg(F.count("*").alias("n_rows"), F.sum("metric").alias("total_metric"))
    .orderBy("source")
)
