from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter(F.col("quantity").between(64, 177))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

agg = (
    filtered_inventory
    .groupBy(
        F.when(F.col("quantity") >= 259, "high").when(F.col("quantity") >= 111, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    agg
    .select("quantity_bucket", "avg_quantity")
    .orderBy(F.col("avg_quantity").desc(), "quantity_bucket")
    .limit(5)
)
