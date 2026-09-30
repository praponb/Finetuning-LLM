from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(F.col("c.signup_date"), F.col("c.email"), F.col("o.store_id"))
    .orderBy(F.col("signup_date").desc())
)
