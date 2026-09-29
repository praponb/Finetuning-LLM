from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_count"),
        F.countDistinct("device").alias("unique_devices"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.firefox_count") > 7)
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("t.firefox_count"),
        F.col("t.unique_devices"),
    )
)
