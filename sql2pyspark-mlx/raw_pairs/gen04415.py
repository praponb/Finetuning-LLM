from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.upper(F.trim(F.col("c.email"))) == "BOB.SMITH@YAHOO.COM")
    .select(
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("c.credit_limit"),
        F.col("c.country"),
    )
)
