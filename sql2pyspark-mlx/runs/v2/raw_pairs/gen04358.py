from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("o.channel"),
        F.col("o.store_id"),
        F.col("c.signup_date"),
        F.col("c.age"),
    )
)
