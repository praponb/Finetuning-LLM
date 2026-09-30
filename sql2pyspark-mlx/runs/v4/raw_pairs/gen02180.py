from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("customer_id").isin([12, 26, 36]))
    .select(
        "*",
        F.when(F.col("browser").isin(["Edge", "Safari"]), "Priority").otherwise("Other").alias("browser_group"),
        F.dayofweek("started_at").alias("started_at_dayofweek"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.signup_date"))
)

not_flagged = (
    with_dims
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_type").isin(["purchase", "page_view"])), "left_anti")
)

result = (
    not_flagged
    .groupBy("device")
    .pivot("country", ["US", "UK"])
    .agg(F.max("started_at_dayofweek").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
