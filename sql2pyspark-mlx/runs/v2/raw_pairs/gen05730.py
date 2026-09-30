from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.uk_count") >= 74)
    .select(F.col("c.id"), F.col("c.age"), F.col("c.signup_date"), F.col("cs.uk_count"))
    .orderBy(F.col("id").desc())
)
