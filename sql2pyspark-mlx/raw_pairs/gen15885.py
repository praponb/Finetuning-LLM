from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.country").isin(["UK", "TH"]) & F.col("c.signup_date").between("2024-05-13", "2025-06-01"))
    .select(F.col("s.id"), F.col("s.started_at"), F.col("s.device"), F.col("c.email"))
)
