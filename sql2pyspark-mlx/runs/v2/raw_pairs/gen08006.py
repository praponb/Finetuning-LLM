from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("c.age"),
        F.col("c.name"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.concat(F.col("c.city"), F.lit(" - "), F.col("c.email")).alias("city_email"),
    )
    .orderBy("name", F.col("city_email").desc())
)
