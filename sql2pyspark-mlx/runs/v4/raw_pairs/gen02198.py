from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("session_count"),
        F.countDistinct("browser").alias("unique_browsers"),
    )
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("S"), F.col("c.id") == F.col("S.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("S.session_count"),
        F.col("S.unique_browsers"),
    )
    .orderBy(F.col("id").desc())
)
