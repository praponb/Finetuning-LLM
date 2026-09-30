from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type", "session_id")
w4 = Window.partitionBy("session_id").orderBy("is_new_session", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy(F.date_trunc("WEEK", "event_time"), "session_id").orderBy("session_id_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("event_type", "session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w7 = Window.partitionBy("session_id", "event_type").orderBy(F.col("cumulative_duration_sec_num").desc_nulls_first(), "id")

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.col("duration_sec").cast("double").alias("duration_sec_num"),
        F.substring("event_type", 1, 2).alias("event_type_prefix"),
        F.coalesce("page", F.lit("N/A")).alias("resolved_page"),
    )
)

prev_events = cleaned.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id_2", F.sum("is_new_session").over(w2))

metrics = (
    sessionized
    .filter(F.col("session_id_2").between(2, 7))
    .select(
        "*",
        F.max("duration_sec_num").over(w3).alias("event_type_max_duration_sec_num"),
        F.avg("session_id_2").over(w4).alias("session_id_2_avg_2_prior"),
        F.sum("duration_sec_num").over(w5).alias("cumulative_duration_sec_num"),
        F.count("duration_sec").over(w6).alias("duration_sec_count_90d"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "session_id",
        "duration_sec_num",
        "event_type_prefix",
        "resolved_page",
        "is_new_session",
        "cumulative_duration_sec_num",
    )
)

enriched = (
    deduped
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.country"), F.col("b.device"), F.col("b.browser"))
)

result = (
    enriched
    .select(
        "id",
        "event_type_prefix",
        "is_new_session",
        "cumulative_duration_sec_num",
        "country",
        "device",
        F.when(F.col("country").rlike("^(JP|DE)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("country_zone"),
    )
    .orderBy(F.col("country_zone").desc())
)
