from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.credit_limit") >= 6700)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("c.email"),
        F.col("c.city"),
        F.col("c.signup_date"),
        F.initcap(F.trim(F.col("s.browser"))).alias("browser_title"),
    )
    .orderBy(F.col("device").desc(), F.col("id").desc())
)
