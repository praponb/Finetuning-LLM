from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.to_date("web_event_event_time")).orderBy("web_event_duration_sec_2", "id")
w4 = Window.orderBy(F.col("web_event_event_time").desc(), "id")
w5 = Window.orderBy("web_event_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("web_event_event_time", "id")
w7 = Window.partitionBy(F.date_trunc("WEEK", "web_event_event_time")).orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_sessions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.country",
        "s.rn",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

latest_match_sessions = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.rn",
        F.col("c.duration_sec").alias("web_event_duration_sec_2"),
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

metrics = (
    latest_match_sessions
    .filter(F.col("web_event_event_time") < "2023-08-01")
    .withColumn("next_web_event_duration_sec_2", F.lead("web_event_duration_sec_2", 1).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("cumulative_web_event_duration_sec_2", F.sum("web_event_duration_sec_2").over(w5))
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("first_known_web_event_duration_sec_2", F.first("web_event_duration_sec_2", ignorenulls=True).over(w7))
)

result = (
    metrics
    .withColumn("first_known_web_event_duration_sec_2_tier", F.when(F.col("first_known_web_event_duration_sec_2") >= 459, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("WEEK", "web_event_event_time"))
    .cube("first_known_web_event_duration_sec_2_tier", "week_start")
    .agg(
        F.min("web_event_event_time").alias("started_at"),
        F.max("web_event_event_time").alias("last_seen"),
        F.max("web_event_event_time").alias("last_seen_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.min("row_seq").alias("min_row_seq"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("avg_row_seq").desc())
)
