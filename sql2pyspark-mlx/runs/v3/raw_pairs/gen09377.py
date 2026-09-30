from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.col("c.city"))
    .agg(
        F.max(F.col("o.order_date")).alias("last_order_date"),
        F.round(((100 * F.sum(F.when(F.col("o.status") == "PENDING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_pending"),
    )
    .orderBy("last_order_date", "city")
    .limit(20)
)
