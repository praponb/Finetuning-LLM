from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .filter(F.col("ct.mobile_count") > 9)
    .select(F.col("c.id"), F.col("c.is_active"), F.col("ct.mobile_count"))
)
