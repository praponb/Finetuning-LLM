from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("term").isin("2024T2", "2024T1"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
    )
)

with_prev = base.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_course_id_running))

scored = (
    sessionized
    .filter(F.col("starts_session").between(0, 1))
    .select(
        "*",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        (F.col("starts_session") - F.col("session_id")).alias("starts_session_minus_session_id"),
    )
)

result = (
    scored
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "prev_enrolled_at",
        "starts_session",
        "session_id",
        "term_len",
        F.to_date(F.col("prev_enrolled_at")).alias("prev_enrolled_at_date"),
    )
    .orderBy("id", F.col("prev_enrolled_at").asc_nulls_last())
)
