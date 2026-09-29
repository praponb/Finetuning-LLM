from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.discount") >= 0.26)
    .groupBy(F.col("c.country"))
    .agg(
        F.max(F.col("o.order_date")).alias("latest_order_date"),
        F.sum(F.when(F.col("o.channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
    .orderBy("country")
)
