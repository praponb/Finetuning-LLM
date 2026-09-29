from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("we.session_id").isin([23, 28, 33]) & F.col("c.email").isNotNull())
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.event_type"),
        F.col("s.country"),
        F.col("s.device"),
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
        F.length(F.col("s.browser")).alias("browser_length"),
    )
)
