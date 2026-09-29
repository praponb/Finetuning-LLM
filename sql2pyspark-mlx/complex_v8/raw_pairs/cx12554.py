from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_session_end_dat = Window.partitionBy("grade_status", F.date_trunc("month", "session_end_date")).orderBy(F.col("duration_min").desc(), "grade_status", "session_no").rowsBetween(-3, Window.currentRow)
w_grade_status_2 = Window.partitionBy("grade_status")
w_grade_status_session_no_by_s = Window.partitionBy("grade_status", "session_no").orderBy("session_end_date", "grade_status", "session_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_prev = filtered.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_grade_status_running))

per_session = (
    sessionized
    .groupBy("grade_status", "session_no")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

tagged = (
    per_session
    .select(
        "grade_status",
        "session_no",
        "session_start",
        "session_end",
        "session_score",
        "duration_min",
        F.to_date(F.col("session_end")).alias("session_end_date"),
    )
)

with_trends = (
    tagged
    .select(
        "grade_status",
        "session_no",
        "session_end",
        "session_score",
        "duration_min",
        F.round(F.avg("duration_min").over(w_grade_status_session_end_dat), 2).alias("duration_min_avg_4"),
        F.count(F.lit(1)).over(w_grade_status_2).alias("rows_in_grade_status"),
        F.max("session_no").over(w_grade_status_session_no_by_s).alias("max_session_no_so_far"),
        F.round(F.col("session_no") / F.nullif(F.sum("session_no").over(w_grade_status_2), F.lit(0)), 4).alias("session_no_share_of_grade_status"),
        F.round(F.col("session_no") - F.avg("session_no").over(w_grade_status_2), 2).alias("session_no_vs_grade_status_avg"),
    )
)

result = (
    with_trends
    .filter(F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN"))
    .select(
        "grade_status",
        "session_no",
        "session_end",
        "session_score",
        "duration_min",
        "duration_min_avg_4",
        "rows_in_grade_status",
        "max_session_no_so_far",
        "session_no_vs_grade_status_avg",
        F.round(F.col("rows_in_grade_status") / F.nullif(F.col("session_no"), F.lit(0)), 3).alias("rows_in_grade_status_per_session_no"),
    )
)
