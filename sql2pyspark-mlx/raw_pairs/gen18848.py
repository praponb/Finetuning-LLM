from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("c.age"),
        F.col("c.is_active"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("age").desc(), "id")
    .limit(20)
)
