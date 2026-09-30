from pyspark.sql import functions as F

by_dimension = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .groupingSets(
        [["country", "event_type"], ["country"], []],
        F.col("p.country").alias("country"),
        F.col("s.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("s.duration_sec")), 2).alias("avg_duration_sec"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    by_dimension
    .select(
        "country",
        "event_type",
        "rows_total",
        "avg_duration_sec",
        F.when(F.col("event_type").rlike("^(add_to_cart|click)$"), "CORE").otherwise("GROUP_A").alias("event_type_zone"),
    )
)
