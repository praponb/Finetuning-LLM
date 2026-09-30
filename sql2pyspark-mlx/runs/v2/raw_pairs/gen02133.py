from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.city").isin(["Bangkok", "New York"]))
    .groupBy(F.col("c.is_active"))
    .agg(
        F.count(F.col("o.amount")).alias("amount_count"),
        F.round(F.avg(F.col("o.shipping_fee")), 2).alias("avg_shipping_fee"),
    )
    .orderBy("amount_count")
)
