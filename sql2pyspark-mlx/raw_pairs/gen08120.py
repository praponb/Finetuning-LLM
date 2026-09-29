from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id")
w3 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy("event_time", "id")
w5 = Window.partitionBy("event_type").orderBy("running_duration_sec", "id")
w6 = Window.partitionBy("event_type").orderBy(F.col("next_duration_sec").asc_nulls_last())
w_part = Window.partitionBy("session_id").orderBy("prev_running_duration_sec_rounded_filled", "id")
w8 = Window.partitionBy("session_id").orderBy("prev_running_duration_sec_rounded", "id")
w9 = Window.partitionBy("session_id").orderBy("prev_running_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("session_id").orderBy(F.col("prev_running_duration_sec_rounded_filled").desc_nulls_last(), "id")

enriched = (
    df_web_events
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w))
    .withColumn("session_id_max_duration_sec", F.max("duration_sec").over(w2))
    .withColumn("duration_sec_avg_5", F.avg("duration_sec").over(w3))
    .withColumn("next_duration_sec", F.lead("duration_sec", 1, 0).over(w4))
)

scored = (
    enriched
    .filter(F.col("next_duration_sec") <= 395)
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec_avg_5",
        "next_duration_sec",
        F.lead("duration_sec_avg_5").over(w5).alias("next_duration_sec_avg_5"),
        F.lag("running_duration_sec").over(w5).alias("prev_running_duration_sec"),
        F.row_number().over(w5).alias("row_seq"),
        F.max("session_id_max_duration_sec").over(w2).alias("session_id_max_session_id_max_duration_sec"),
    )
)

cleaned = (
    scored
    .filter(F.col("session_id_max_session_id_max_duration_sec").between(126, 441))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec_avg_5",
        "next_duration_sec",
        "prev_running_duration_sec",
        "row_seq",
        "session_id_max_session_id_max_duration_sec",
        F.round("prev_running_duration_sec", 1).alias("prev_running_duration_sec_rounded"),
    )
)

valid = (
    cleaned
    .filter(F.col("event_time") > F.lit("2023-07-11 12:00:00").cast("timestamp"))
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec_avg_5",
        "next_duration_sec",
        "prev_running_duration_sec",
        "session_id_max_session_id_max_duration_sec",
        "prev_running_duration_sec_rounded",
        F.coalesce("prev_running_duration_sec_rounded", F.lit(0)).alias("prev_running_duration_sec_rounded_filled"),
    )
)

scored_web_events = (
    valid
    .select(
        "id",
        "session_id",
        "prev_running_duration_sec_rounded",
        "prev_running_duration_sec_rounded_filled",
        F.rank().over(w6).alias("next_duration_sec_rank"),
        F.row_number().over(w_part).alias("seq_no"),
        F.lead("session_id_max_session_id_max_duration_sec", 2).over(w8).alias("next_session_id_max_session_id_max_duration_sec"),
        F.sum("prev_running_duration_sec_rounded").over(w9).alias("running_prev_running_duration_sec_rounded"),
        F.min("prev_running_duration_sec_rounded_filled").over(w2).alias("session_id_min_prev_running_duration_sec_rounded_filled"),
    )
)

latest = (
    scored_web_events
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

base = (
    latest
    .filter(F.col("next_duration_sec_rank") == 6)
    .withColumn("prev_running_duration_sec_rounded_filled_int", F.col("prev_running_duration_sec_rounded_filled").cast("int"))
    .withColumn("next_session_id_max_session_id_max_duration_sec_per_session_id_min_prev_running_duration_sec_rounded_filled", (F.col("next_session_id_max_session_id_max_duration_sec") / F.nullif("session_id_min_prev_running_duration_sec_rounded_filled", F.lit(0))))
)

totals = (
    base
    .groupBy(
        F.when(F.col("prev_running_duration_sec_rounded_filled_int") >= 76, "LARGE").otherwise("SMALL").alias("prev_running_duration_sec_rounded_filled_int_tier"),
    )
    .agg(
        F.percentile_approx("prev_running_duration_sec_rounded_filled_int", [0.25, 0.5, 0.75]).alias("prev_running_duration_sec_rounded_filled_int_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    totals
    .select(
        "prev_running_duration_sec_rounded_filled_int_tier",
        "prev_running_duration_sec_rounded_filled_int_quartiles",
    )
    .orderBy(F.col("prev_running_duration_sec_rounded_filled_int_tier").desc())
)
