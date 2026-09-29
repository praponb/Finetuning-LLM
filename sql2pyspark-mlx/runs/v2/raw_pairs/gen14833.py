from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(F.upper(F.col("c.email")).like("%IAM%"))
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("c.is_active"),
        F.initcap(F.trim(F.col("c.city"))).alias("city_title"),
    )
    .orderBy(F.col("customer_id").desc())
)
