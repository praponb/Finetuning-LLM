from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id")
w_student_id_by_score = Window.partitionBy("student_id").orderBy(F.col("score").desc(), "id")
w_student_id_by_score_running = Window.partitionBy("student_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_enrollments
    .filter((F.col("term") == "2025T1") | (F.col("grade_status").isin("INCOMPLETE", "PASS", "WITHDRAWN")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

sequenced = (
    cleaned
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "enrolled_at_label",
        F.round(F.avg("score").over(w_student_id), 2).alias("student_id_avg_score"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_id_by_score))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.sum("term_len").over(w_student_id_by_score_running).alias("running_term_len"),
    )
)

exploded_detail = (
    sequenced.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.course_id",
        "s.enrolled_at",
        "s.minutes_since_prev_enrolled_at",
        "s.running_term_len",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.term").alias("enrollment_term"),
    )
)

labeled = (
    exploded_detail
    .withColumn("enrollment_grade_status_prefix", F.substring(F.trim(F.col("enrollment_grade_status")), 1, 4))
)

enrollment_student_id_stats = (
    labeled
    .groupBy("enrollment_student_id")
    .agg(F.percentile_approx("running_term_len", 0.5).alias("median_running_term_len"))
)

vs_group = (
    labeled.alias("s")
    .join(enrollment_student_id_stats.alias("g"), F.col("g.enrollment_student_id") == F.col("s.enrollment_student_id"), "inner")
    .filter(F.col("s.running_term_len") >= F.col("g.median_running_term_len"))
    .select(
        "s.*",
        "g.median_running_term_len",
        F.round(F.col("s.running_term_len") - F.col("g.median_running_term_len"), 2).alias("running_term_len_minus_median"),
    )
)

result = (
    vs_group
    .filter(F.col("enrollment_grade_status").isNotNull())
    .select(
        "id",
        "running_term_len",
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_term",
        "enrollment_grade_status_prefix",
        "median_running_term_len",
        "running_term_len_minus_median",
        F.substring(F.trim(F.col("enrollment_grade_status")), 1, 4).alias("enrollment_grade_status_prefix_2"),
    )
    .orderBy(F.col("running_term_len_minus_median").desc())
)
