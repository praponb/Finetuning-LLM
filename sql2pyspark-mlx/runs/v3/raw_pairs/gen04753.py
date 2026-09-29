from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(~F.col("c.is_active"))
    .groupBy(
        F.col("o.customer_id"),
        F.when(F.col("o.shipping_fee") >= 32, "high").when(F.col("o.shipping_fee") >= 12, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.avg(F.col("s.cost")).alias("avg_cost"),
        F.round(((100 * F.sum(F.when(F.col("s.carrier") == "DHL", 1).otherwise(0))) / F.count("*")), 1).alias("pct_dhl"),
    )
)
