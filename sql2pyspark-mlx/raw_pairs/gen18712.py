from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.col("customer_id").isin([1, 24]))
    .groupBy("customer_id")
    .agg(
        F.countDistinct("device").alias("unique_devices"),
        F.sum(F.when(F.col("browser") == "Edge", 1).otherwise(0)).alias("edge_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(
        F.col("c.city"),
        F.col("c.is_active"),
        F.col("t.unique_devices"),
        F.col("t.edge_count"),
    )
    .orderBy("city")
)
