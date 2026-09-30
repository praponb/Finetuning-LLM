from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 100)
    .groupBy("store_id", "product_id")
    .agg(F.countDistinct("quantity").alias("unique_stores"))
    .filter(F.col("unique_stores") >= 6)
    .select("store_id", "product_id", "unique_stores")
    .orderBy(F.col("store_id").desc())
)
