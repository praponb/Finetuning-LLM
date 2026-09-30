from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter((F.year(F.col("c.signup_date")) == 2025) & F.col("s.device").isin(["mobile", "tablet"]))
    .select(
        F.col("s.customer_id"),
        F.col("c.segment"),
        F.col("c.name"),
        F.col("c.is_active"),
    )
    .orderBy("name")
)
