from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.countDistinct("device").alias("unique_devices"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.unique_devices") >= 6)
    .select(F.col("c.id"), F.col("c.name"), F.col("c.age"), F.col("t.unique_devices"))
    .orderBy(F.col("age").desc())
)
