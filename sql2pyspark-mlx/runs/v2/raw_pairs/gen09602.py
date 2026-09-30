from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .groupBy(F.col("s.region"))
    .agg(
        F.avg(F.col("o.discount")).alias("avg_discount"),
        F.sum(F.when(F.col("o.channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
    .filter(F.col("mobile_count") > 21)
)
