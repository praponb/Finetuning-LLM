from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([13, 32, 37, 39]))
    .groupBy("supplier_id")
    .agg(
        F.count("price").alias("price_count"),
        F.sum("price").alias("total_price"),
    )
    .orderBy("supplier_id")
)
