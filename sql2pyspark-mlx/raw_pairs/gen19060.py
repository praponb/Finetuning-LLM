from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter(F.to_date(F.col("s.started_at")) >= "2023-09-01")
    .groupBy(F.col("c.segment"))
    .agg(F.min(F.col("s.started_at")).alias("first_started_at"))
    .orderBy(F.col("first_started_at").desc(), "segment")
)
