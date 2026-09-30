from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("status") == "PENDING").alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_semi")
    .filter(F.col("c.segment") == "Corporate")
    .select(F.col("c.id"), F.col("c.credit_limit"), F.col("c.segment"), F.col("c.age"))
    .orderBy(F.col("credit_limit").desc())
)
