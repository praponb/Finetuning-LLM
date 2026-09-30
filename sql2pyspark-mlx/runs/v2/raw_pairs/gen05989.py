from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "SHIPPED") & (F.col("store_id").isin([15, 18, 22, 26]) | (F.col("store_id") == 36)))
    .groupBy("channel")
    .agg(F.max("order_date").alias("last_order_date"))
    .orderBy("channel", "last_order_date")
)
