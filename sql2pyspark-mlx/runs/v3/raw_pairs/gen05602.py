from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.order_date").between("2023-03-01", "2025-01-24"))
    .groupBy(
        F.col("c.segment"),
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(
        (F.max(F.col("o.shipping_fee")) - F.min(F.col("o.shipping_fee"))).alias("shipping_fee_range"),
    )
    .filter(F.col("shipping_fee_range") > 48)
)
