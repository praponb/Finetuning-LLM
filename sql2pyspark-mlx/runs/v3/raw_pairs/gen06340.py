from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .groupBy(
        F.when(F.col("s.device").isin(["desktop", "tablet"]), "tier_1").otherwise("other").alias("device_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("we.page") == "/checkout", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checkout"),
    )
)
