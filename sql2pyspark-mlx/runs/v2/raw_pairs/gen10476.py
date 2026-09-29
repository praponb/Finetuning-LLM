from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("c.is_active"),
        F.col("c.age"),
        F.col("c.email"),
        F.col("s.browser"),
        F.when(F.col("c.city") == "Chiang Mai", 1).otherwise(0).alias("is_chiang_mai"),
    )
    .orderBy("browser", F.col("email").desc())
)
