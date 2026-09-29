from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-08-01", "2025-08-01") | F.col("quantity").isNotNull())
    .groupBy("store_id", "product_id")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") < 3)
    .orderBy(F.col("cnt").desc(), F.col("store_id").desc())
)
