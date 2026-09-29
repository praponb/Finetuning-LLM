from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 4) & (F.col("product_id").isin([3, 5, 7]) | (F.year("last_restock") >= 2024)))
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("cnt").desc())
)
