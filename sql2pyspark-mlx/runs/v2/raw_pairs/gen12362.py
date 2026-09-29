from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.segment").isNotNull())
    .select(
        F.col("c.signup_date"),
        F.col("c.city"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("o.discount"),
    )
    .orderBy("city")
)
