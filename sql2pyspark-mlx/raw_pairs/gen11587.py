from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(~(~F.col("s.browser").isin(["Chrome", "Firefox", "Edge"]) & F.col("s.country").isNotNull()))
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("c.segment"),
        F.col("c.city"),
    )
    .orderBy(F.col("id").desc())
)
