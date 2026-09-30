from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 6]))
    .select("id", "product_id")
    .orderBy(F.col("id").desc(), F.col("product_id").desc())
    .limit(10)
)
