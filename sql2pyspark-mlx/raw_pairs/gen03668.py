from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 7, 10]))
    .groupBy("product_id", "store_id")
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 4)
    .select("product_id", "store_id", "last_last_restock", "cnt")
    .orderBy("store_id", F.col("cnt").desc(), "product_id")
    .limit(20)
)
