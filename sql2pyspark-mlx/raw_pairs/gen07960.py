from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .filter(F.col("customer_id") == 40)
    .groupBy("customer_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(F.col("c.country"), F.col("cs.cnt"))
    .orderBy(F.col("cnt").desc())
)
