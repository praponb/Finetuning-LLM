from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter((F.col("product_id") == 10) & (F.col("quantity") > 115) & F.col("store_id").isin([3, 6]))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)

store_id_summary = (
    recent_inventory
    .groupBy("store_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    store_id_summary
    .filter(F.col("total_quantity") < 960)
    .select("store_id", "total_quantity")
    .orderBy(F.col("total_quantity").desc(), "store_id")
    .limit(20)
)
