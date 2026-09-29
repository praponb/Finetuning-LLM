from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.name").like("%Olivi%"))
    .select(
        F.col("c.signup_date"),
        F.col("s.customer_id"),
        F.upper(F.col("c.name")).alias("name_upper"),
    )
)
