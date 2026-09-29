from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(F.col("c.signup_date"), F.col("c.name"), F.col("cs.cnt"))
    .orderBy("cnt")
)
