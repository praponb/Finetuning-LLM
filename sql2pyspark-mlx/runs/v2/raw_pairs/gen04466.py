from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_anti")
    .filter(F.lower(F.col("c.name")).like("%ia %"))
    .select(F.col("c.id"), F.col("c.age"), F.col("c.segment"), F.col("c.signup_date"))
)
