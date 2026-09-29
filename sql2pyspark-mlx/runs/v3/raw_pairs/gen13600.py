from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.col("c.country"))
    .agg(
        F.sum(F.when(F.col("s.device") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
    .orderBy("country")
)
