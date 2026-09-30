from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_count_by_duration_min = Window.partitionBy("event_count").orderBy("duration_min_distance", "quality", "session_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_2 = Window.partitionBy("quality")
w_event_count_by_duration_min_2 = Window.partitionBy("event_count").orderBy("duration_min_distance", "quality", "session_no").rowsBetween(-3, Window.currentRow)
w_event_count_by_duration_min_3 = Window.partitionBy("event_count").orderBy("duration_min_distance", "quality", "session_no").rowsBetween(-6, -1)

base = (
    df_meter_readings
    .filter(F.col("voltage") > 214.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_prev = base.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_quality_running))

sessions = (
    sessionized
    .groupBy("quality", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    sessions
    .filter(F.col("session_no") >= 2)
    .select(
        "*",
        (F.col("duration_min") - F.col("event_count")).alias("duration_min_minus_event_count"),
        F.abs(F.col("duration_min") - 120.0).alias("duration_min_distance"),
    )
)

sequenced = (
    annotated
    .select(
        "quality",
        "session_no",
        "session_start",
        "session_end",
        "session_kwh",
        "duration_min_minus_event_count",
        "duration_min_distance",
        F.min("session_kwh").over(w_event_count_by_duration_min).alias("min_session_kwh_so_far"),
        F.count(F.lit(1)).over(w_quality_2).alias("rows_in_quality"),
        F.round(F.col("duration_min") / F.nullif(F.sum("duration_min").over(w_quality_2), F.lit(0)), 4).alias("duration_min_share_of_quality"),
        F.round(F.avg("duration_min_distance").over(w_event_count_by_duration_min_2), 2).alias("duration_min_distance_avg_4"),
        F.round(F.avg("duration_min_distance").over(w_event_count_by_duration_min_3), 2).alias("duration_min_distance_avg_6_prior"),
        F.round(F.col("session_kwh") - F.avg("session_kwh").over(w_quality_2), 2).alias("session_kwh_vs_quality_avg"),
    )
)

result = (
    sequenced
    .select(
        "quality",
        "session_no",
        F.col("session_kwh_vs_quality_avg").alias("m_session_kwh_vs_quality_avg"),
        F.col("session_kwh").alias("session_kwh_v"),
        F.col("min_session_kwh_so_far").alias("min_session_kwh_so_far_v"),
    )
    .unpivot(
        ids=["quality", "session_no"],
        values=["m_session_kwh_vs_quality_avg", "session_kwh_v", "min_session_kwh_so_far_v"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("quality", "session_no", "field")
)
