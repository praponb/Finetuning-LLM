from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.country").isin(["SG", "JP"]))
    .groupBy(
        F.month(F.col("c.signup_date")).alias("signup_month"),
        F.col("c.is_active"),
    )
    .agg(F.round(F.stddev(F.col("o.shipping_fee")), 2).alias("stddev_shipping_fee"))
    .filter(F.col("stddev_shipping_fee") > 34)
)
