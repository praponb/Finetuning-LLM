from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "sale") & (F.col("cost") <= 550))
    .select("price", F.col("cost").cast("int").alias("cost_int"))
    .orderBy(F.col("cost_int").desc(), F.col("price").desc())
)
