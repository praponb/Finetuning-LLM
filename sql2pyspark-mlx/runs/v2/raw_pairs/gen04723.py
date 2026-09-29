from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(~F.col("browser").isin(["Safari", "Firefox"]))
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.credit_limit"), F.col("cs.jp_count"))
    .orderBy("id")
)
