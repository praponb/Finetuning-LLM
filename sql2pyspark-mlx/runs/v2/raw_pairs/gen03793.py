from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.sum(F.when(F.col("country") == "JP", 1).otherwise(0)).alias("jp_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.age"), F.col("cs.jp_count"))
    .orderBy(F.col("age").desc(), F.col("id").desc())
    .limit(5)
)
