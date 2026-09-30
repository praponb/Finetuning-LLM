from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .select(F.col("c.age"), F.col("c.signup_date"), F.col("o.amount"))
    .orderBy(F.col("amount").desc())
)
