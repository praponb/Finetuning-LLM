from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.countDistinct("device").alias("unique_devices"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.unique_devices") >= 3)
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("c.signup_date"),
        F.col("t.unique_devices"),
    )
    .orderBy(F.col("signup_date").desc())
)
