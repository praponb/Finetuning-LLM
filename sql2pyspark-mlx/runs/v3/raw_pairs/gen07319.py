from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.signup_date") >= "2025-05-01")
    .select(
        F.col("c.segment"),
        F.col("c.credit_limit"),
        F.col("c.email"),
        F.col("o.discount"),
        F.col("o.store_id"),
    )
    .orderBy("segment")
)
