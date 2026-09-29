from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.col("c.is_active"),
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(
        F.min(F.col("o.order_date")).alias("min_order_date"),
        F.countDistinct(F.col("o.amount")).alias("unique_customers"),
    )
)
