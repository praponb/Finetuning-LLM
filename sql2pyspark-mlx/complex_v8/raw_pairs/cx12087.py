from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_len_by_total_en = Window.partitionBy("grade_status_len").orderBy(F.col("total_enrollment_count").desc(), "grade_status", "enrollment_count", "course_id").rowsBetween(-2, Window.currentRow)
w_grade_status_len_by_total_en_2 = Window.partitionBy("grade_status_len").orderBy(F.col("total_enrollment_count").desc(), "grade_status", "enrollment_count", "course_id").rowsBetween(-3, -1)
w_grade_status_len_by_cnt = Window.partitionBy("grade_status_len").orderBy("cnt", "grade_status", "enrollment_count", "course_id")

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("grade_status").isin("FAIL", "INCOMPLETE"), 1)).alias("enrollment_grade_status_count"),
        F.count("*").alias("enrollment_count"),
    )
)

summarized_rows = (
    cleaned.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

student_id_stats = (
    summarized_rows
    .groupBy("student_id")
    .agg(F.max("enrollment_grade_status_count").alias("max_enrollment_grade_status_count"))
)

vs_group = (
    summarized_rows.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.enrollment_grade_status_count") == F.col("g.max_enrollment_grade_status_count"))
    .select(
        "s.*",
        "g.max_enrollment_grade_status_count",
        F.round(F.col("s.enrollment_grade_status_count") - F.col("g.max_enrollment_grade_status_count"), 2).alias("enrollment_grade_status_count_minus_max"),
    )
)

per_group = (
    vs_group
    .filter(F.hour(F.col("enrolled_at")).between(10, 15))
    .groupBy("grade_status", "enrollment_count", "course_id")
    .agg(
        F.round(F.stddev("enrollment_count"), 2).alias("stddev_enrollment_count"),
        F.sum("enrollment_count").alias("total_enrollment_count"),
        F.count("*").alias("cnt"),
    )
)

labeled = (
    per_group
    .select(
        "grade_status",
        "enrollment_count",
        "course_id",
        "total_enrollment_count",
        "cnt",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

windowed = (
    labeled
    .select(
        "grade_status",
        "enrollment_count",
        "course_id",
        "cnt",
        F.round(F.avg("enrollment_count").over(w_grade_status_len_by_total_en), 2).alias("enrollment_count_avg_3"),
        F.round(F.avg("enrollment_count").over(w_grade_status_len_by_total_en_2), 2).alias("enrollment_count_avg_3_prior"),
        F.lag("total_enrollment_count").over(w_grade_status_len_by_cnt).alias("prev_total_enrollment_count"),
    )
)

result = (
    windowed
    .select(
        "grade_status",
        "enrollment_count",
        "course_id",
        "enrollment_count_avg_3",
        "enrollment_count_avg_3_prior",
        "prev_total_enrollment_count",
    )
    .orderBy(F.col("enrollment_count").desc(), "enrollment_count_avg_3_prior")
)
