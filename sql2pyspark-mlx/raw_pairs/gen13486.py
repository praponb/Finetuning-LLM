from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter(F.col("last_restock") < "2024-05-24")
    .withColumn("last_restock_period", F.date_trunc("MONTH", "last_restock"))
)

summary = (
    filtered_inventory
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = summary.orderBy("last_restock_month")
