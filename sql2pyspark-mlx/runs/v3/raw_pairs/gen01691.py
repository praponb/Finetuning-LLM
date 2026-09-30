from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .groupBy("customer_id")
    .agg(F.sum(F.when(F.col("country") == "TH", 1).otherwise(0)).alias("th_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.th_count") > 64)
    .select(F.col("c.id"), F.col("c.segment"), F.col("ct.th_count"))
    .orderBy("th_count")
)
