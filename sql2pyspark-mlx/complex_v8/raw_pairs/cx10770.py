from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_session_end_mon = Window.partitionBy("grade_status", F.month("session_end")).orderBy("event_count", "grade_status", "burst_id")
w_all_by_event_count = Window.orderBy(F.col("event_count").desc_nulls_first(), "grade_status", "burst_id")
w_grade_status_session_end_mon_2 = Window.partitionBy("grade_status", F.month("session_end")).orderBy("event_count", "grade_status", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

with_prev = prepared.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_grade_status_running))

per_session = (
    sessionized
    .groupBy("grade_status", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    per_session
    .select(
        "*",
        F.ntile(10).over(w_grade_status_session_end_mon).alias("event_count_decile"),
        F.round(F.percent_rank().over(w_all_by_event_count), 3).alias("event_count_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_event_count), 3).alias("event_count_cume_dist"),
        F.lag("event_count").over(w_all_by_event_count).alias("prev_event_count"),
        F.last("session_start", ignorenulls=True).over(w_grade_status_session_end_mon_2).alias("last_known_session_start"),
    )
)

result = (
    metrics
    .select(
        "grade_status",
        "burst_id",
        "event_count",
        "duration_min",
        "event_count_decile",
        "event_count_percent_rank",
        "event_count_cume_dist",
        "prev_event_count",
        "last_known_session_start",
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
    .orderBy(F.col("event_count_percent_rank").desc())
)
