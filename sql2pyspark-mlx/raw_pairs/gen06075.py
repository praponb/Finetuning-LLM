from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        (F.max(F.col("o.shipping_fee")) - F.min(F.col("o.shipping_fee"))).alias("shipping_fee_range"),
        F.round(F.avg(F.col("o.discount")), 2).alias("avg_discount"),
    )
    .filter(F.col("shipping_fee_range") >= 40)
    .orderBy("signup_month")
)
