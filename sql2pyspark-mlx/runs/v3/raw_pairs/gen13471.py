from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.city"), F.col("c.credit_limit"), F.col("t.th_count"))
    .orderBy(F.col("credit_limit").desc())
)
