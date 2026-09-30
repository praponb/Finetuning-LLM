from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("country") == "US", 1)).alias("us_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .filter(F.col("ct.us_count") > 3)
    .select(F.col("c.is_active"), F.col("ct.us_count"))
    .orderBy(F.col("us_count").desc())
)
