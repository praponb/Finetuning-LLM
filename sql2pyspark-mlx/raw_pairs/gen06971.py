from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.filter(F.col("signup_date") >= "2025-02-12").alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_semi")
    .select(F.col("o.id"), F.col("o.store_id"))
    .orderBy(F.col("store_id").desc(), F.col("id").desc())
    .limit(10)
)
