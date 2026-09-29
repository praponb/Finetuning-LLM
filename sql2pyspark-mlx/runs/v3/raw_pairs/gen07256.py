from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.avg("price").alias("avg_price"),
        (F.max("price") - F.min("price")).alias("price_range"),
    )
    .filter(F.col("avg_price") < 920)
    .orderBy(F.col("supplier_id").desc())
)
