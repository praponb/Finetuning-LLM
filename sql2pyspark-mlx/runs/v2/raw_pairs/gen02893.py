from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.countDistinct("customer_id").alias("unique_customers"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.unique_customers") >= 6)
    .select(F.col("c.credit_limit"), F.col("cs.unique_customers"))
)
