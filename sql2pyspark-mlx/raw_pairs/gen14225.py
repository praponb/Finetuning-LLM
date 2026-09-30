from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & ~F.col("device").isin(["tablet", "desktop"]))
    .withColumn("started_at_date", F.to_date("started_at"))
    .withColumn("resolved_country", F.coalesce("country", F.lit("N/A")))
)

result = (
    valid
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at_date")), "left_anti")
)
