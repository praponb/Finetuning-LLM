from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("s.customer_id").isNull() | (F.col("s.country") == "SG"))
    .select(
        F.col("s.device"),
        F.col("s.browser"),
        F.col("c.name"),
        F.round((F.col("c.credit_limit") * 100), 2).alias("credit_limit_adjusted"),
    )
)
