from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("customer_id") == 5)
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(F.col("c.signup_date"), F.col("ct.sg_count"))
)
