from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.countDistinct("device").alias("unique_devices"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(F.col("c.country"), F.col("c.segment"), F.col("C.unique_devices"))
    .orderBy("segment")
)
