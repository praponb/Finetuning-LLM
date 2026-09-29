from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.to_date("started_at") < "2025-05-02")
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.min("started_at").alias("first_started_at"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_totals.alias("customer_totals"), F.col("customers.id") == F.col("customer_totals.customer_id"), "inner")
    .filter(F.col("customer_totals.desktop_count") >= 6)
    .select(
        F.col("customers.id"),
        F.col("customers.city"),
        F.col("customer_totals.desktop_count"),
        F.col("customer_totals.first_started_at"),
    )
    .orderBy("city")
)
