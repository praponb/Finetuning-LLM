from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("o.store_id") == 15)
    .groupBy(
        F.col("c.is_active"),
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
    )
    .agg(
        F.sum(F.col("o.amount")).alias("total_amount"),
        F.max(F.col("o.shipping_fee")).alias("max_shipping_fee"),
    )
)
