from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.status"),
        F.col("o.customer_id"),
        F.col("c.city"),
        F.concat(F.col("c.country"), F.lit("/"), F.col("c.email")).alias("country_email"),
    )
)
