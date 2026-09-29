from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.signup_date") < "2025-01-25")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.sum(F.when(F.col("s.device") == "tablet", 1).otherwise(0)).alias("tablet_count"),
    )
)
