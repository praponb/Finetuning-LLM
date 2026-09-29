from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("o.discount") > 0.27)
    .select(
        F.col("c.city"),
        F.col("c.signup_date"),
        F.col("c.country"),
        F.col("o.discount"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
    )
)
