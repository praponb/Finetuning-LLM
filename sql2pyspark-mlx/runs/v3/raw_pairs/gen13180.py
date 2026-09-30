from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"), F.col("s.region"))
    .agg(
        F.sum(F.when(F.col("o.channel") == "mobile", F.col("o.amount")).otherwise(0)).alias("mobile_amount"),
        F.sum(F.when(F.col("o.status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
    )
    .orderBy("shipped_count")
)
