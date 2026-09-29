from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter((F.col("s.device") == "tablet") & ~F.col("c.segment").isin(["Consumer", "Corporate", "SMB"]))
    .select(F.col("s.customer_id"), F.col("c.age"), F.col("c.credit_limit"))
    .orderBy(F.col("age").desc())
)
