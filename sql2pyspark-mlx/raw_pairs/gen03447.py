from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.name").like("%Lee%"))
    .groupBy(
        F.date_trunc("MONTH", F.col("customers.signup_date")).alias("signup_month"),
    )
    .agg(
        (F.max(F.col("orders.shipping_fee")) - F.min(F.col("orders.shipping_fee"))).alias("shipping_fee_range"),
    )
    .orderBy("signup_month", F.col("shipping_fee_range").desc())
    .limit(1)
)
