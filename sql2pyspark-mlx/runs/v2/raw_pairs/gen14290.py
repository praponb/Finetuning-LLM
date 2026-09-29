from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("we.event_type").isin(["purchase", "click", "page_view"]))
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"), F.col("c.segment"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("we.page") == "/checkout", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checkout"),
    )
    .filter(F.col("pct_checkout") >= 46)
    .orderBy(F.col("pct_checkout").desc())
)
