from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([3, 6, 8]))
    .select("order_date")
    .unionByName(df_inventory.select(F.col("last_restock").alias("order_date")))
    .distinct()
    .orderBy("order_date")
)
