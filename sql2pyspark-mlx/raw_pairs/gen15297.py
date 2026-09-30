from pyspark.sql import functions as F

rollup_totals = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "left")
    .groupingSets(
        [["country", "device", "page"], ["country", "device"], ["country"], []],
        F.col("d.country").alias("country"),
        F.col("d.device").alias("device"),
        F.col("a.page").alias("page"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.duration_sec")).alias("max_duration_sec"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    rollup_totals
    .select(
        "country",
        "device",
        "page",
        "max_duration_sec",
        "gid",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "GROUP_B").when(F.col("device").rlike("^(mobile)$"), "EXTENDED").otherwise("GROUP_A").alias("device_zone"),
    )
)
