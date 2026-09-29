from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 4, 8]))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
    .orderBy(F.col("days_since_last_restock").desc(), F.col("id").desc())
)
