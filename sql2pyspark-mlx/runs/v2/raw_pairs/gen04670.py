from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Edge", "Chrome"]))
    .groupBy("customer_id")
    .agg(F.countDistinct("browser").alias("unique_browsers"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .filter(F.col("sbc.unique_browsers") > 9)
    .select(F.col("c.id"), F.col("c.city"), F.col("sbc.unique_browsers"))
    .orderBy(F.col("unique_browsers").desc())
)
