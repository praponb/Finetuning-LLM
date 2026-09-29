from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at_weekofyear", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.month("started_at"), "browser")
w5 = Window.partitionBy("device", "web_event_event_type")
w6 = Window.partitionBy("browser").orderBy("started_at_weekofyear", "id")
w7 = Window.partitionBy("country", "web_event_event_type").orderBy(F.col("next_started_at_weekofyear").desc(), "id")

filtered = (
    df_sessions
    .filter(~F.col("country").eqNullSafe(F.lit("SG")))
    .withColumn("started_at_weekofyear", F.weekofyear("started_at"))
)

enriched = (
    filtered
    .filter(F.col("country") == "UK")
    .select(
        "*",
        F.lead("started_at_weekofyear").over(w).alias("next_started_at_weekofyear"),
        F.row_number().over(w).alias("row_seq"),
        F.sum("started_at_weekofyear").over(w2).alias("cumulative_started_at_weekofyear"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

enriched_sessions = (
    with_last
    .select(
        "id",
        "device",
        "country",
        "started_at_weekofyear",
        "next_started_at_weekofyear",
        "row_seq",
        "cumulative_started_at_weekofyear",
        "web_event_event_time",
        "web_event_event_type",
        F.max("cumulative_started_at_weekofyear").over(w4).alias("started_at_month_max_cumulative_started_at_weekofyear"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w5)), 4).alias("row_seq_share"),
        F.lead("row_seq", 2, 0).over(w6).alias("next_row_seq"),
    )
)

latest = (
    enriched_sessions
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

grouped = (
    latest
    .groupBy("device")
    .agg(
        F.percentile_approx("started_at_month_max_cumulative_started_at_weekofyear", [0.25, 0.5, 0.75]).alias("started_at_month_max_cumulative_started_at_weekofyear_quartiles"),
        F.sum(F.when(F.col("country") == "SG", F.col("row_seq_share")).otherwise(0)).alias("sg_row_seq_share"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
)

result = (
    grouped
    .select(
        "device",
        "started_at_month_max_cumulative_started_at_weekofyear_quartiles",
        "num_countries",
    )
    .orderBy(F.col("device").asc_nulls_last())
)
