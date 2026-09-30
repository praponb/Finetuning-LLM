from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.name").like("N%") | (F.lower(F.trim(F.col("c.email"))) == "bob.smith@yahoo.com"))
    .select(
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("c.credit_limit"),
        F.col("c.name"),
    )
)
