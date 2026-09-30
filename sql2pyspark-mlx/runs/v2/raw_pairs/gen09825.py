from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(~F.col("country").isin(["SG", "DE", "TH"]))
    .groupBy("customer_id")
    .agg(F.countDistinct("device").alias("unique_devices"))
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "inner")
    .filter(F.col("customer_stats.unique_devices") >= 9)
    .select(
        F.col("customers.signup_date"),
        F.col("customers.city"),
        F.col("customer_stats.unique_devices"),
    )
)
