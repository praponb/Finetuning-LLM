from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("c.age"),
        F.col("c.name"),
        F.coalesce(F.col("s.country"), F.lit("unassigned")).alias("country_filled"),
    )
    .orderBy(F.col("id").desc())
    .limit(3)
)
