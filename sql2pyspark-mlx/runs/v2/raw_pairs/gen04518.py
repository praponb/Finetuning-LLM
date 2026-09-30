from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["SHIPPED", "CANCELLED", "PENDING"]) & ~F.col("channel").isin(["partner", "store"]))
    .groupBy("store_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.max("order_date").alias("last_order_date"),
    )
    .filter(F.col("total_amount") > 7200)
)
