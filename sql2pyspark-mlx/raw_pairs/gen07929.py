from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("s.city"),
        F.col("c.credit_limit"),
        F.concat(F.col("c.name"), F.lit(" "), F.col("c.email")).alias("name_email"),
    )
)
