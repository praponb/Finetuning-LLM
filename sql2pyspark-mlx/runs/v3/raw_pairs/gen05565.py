from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.age").between(32, 67))
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("c.age"),
        F.col("c.credit_limit"),
        F.when(F.col("c.segment").isin(["Consumer", "Corporate"]), "Group A").otherwise("Other").alias("segment_group"),
    )
    .orderBy("segment_group")
)
