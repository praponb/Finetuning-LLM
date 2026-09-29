from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(~F.col("country").isin(["JP", "US"]))
    .groupBy("customer_id")
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "inner")
    .select(
        F.col("customers.name"),
        F.col("customer_stats.last_started_at"),
        F.col("customer_stats.row_count"),
    )
)
