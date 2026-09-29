from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.sum(F.when(F.col("country") == "SG", 1).otherwise(0)).alias("sg_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.sg_count") > 29)
    .select(F.col("c.id"), F.col("c.age"), F.col("c.is_active"), F.col("t.sg_count"))
)
