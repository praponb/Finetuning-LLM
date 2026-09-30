from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(F.col("signup_date").between("2024-08-01", "2024-08-09")).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("s.country"),
    )
)
