from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.col("c.city") == "Berlin")
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.city"),
        F.col("c.signup_date"),
    )
)
