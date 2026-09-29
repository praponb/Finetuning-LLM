from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(
        F.max(F.col("s.started_at")).alias("latest_started_at"),
        F.countDistinct(F.col("s.country")).alias("unique_countrys"),
    )
    .orderBy(F.col("unique_countrys").desc(), "signup_quarter")
    .limit(5)
)
