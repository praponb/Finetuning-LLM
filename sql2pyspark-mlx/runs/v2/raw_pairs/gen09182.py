from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("browser") == "Edge")
    .groupBy("customer_id")
    .agg(
        F.countDistinct("device").alias("unique_devices"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("ct.unique_devices"),
        F.col("ct.de_count"),
    )
)
