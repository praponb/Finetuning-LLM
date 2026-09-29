from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.customer_id") == 21)
    .groupBy(
        F.col("c.is_active"),
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(
        F.max(F.col("o.shipping_fee")).alias("max_shipping_fee"),
        F.count("*").alias("row_count"),
    )
)
