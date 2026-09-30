from pyspark.sql import functions as F

dim_totals = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .groupingSets(
        [["country", "event_type"], ["country"], []],
        F.col("s.country").alias("country"),
        F.col("e.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("e.duration_sec")).alias("avg_duration_sec"),
    )
)

result = (
    dim_totals
    .select(
        "country",
        "rows_total",
        "avg_duration_sec",
        F.when(F.col("country").rlike("^(US|SG)$"), "CORE").when(F.col("country").rlike("^(TH|JP)$"), "PRIORITY").otherwise("GROUP_A").alias("country_zone"),
    )
    .orderBy(F.col("country_zone").desc(), F.col("rows_total").desc())
)
