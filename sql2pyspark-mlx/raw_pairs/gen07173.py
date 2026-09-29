from pyspark.sql import functions as F

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.session_id").isin([15, 21])), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.when(F.col("country").rlike("^(SG|JP)$"), "CORE").when(F.col("country").rlike("^(TH|US)$"), "PRIORITY").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy(F.col("country").desc(), "id")
)
