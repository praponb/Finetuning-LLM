from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_grade_status_or_un = Window.partitionBy("course_id", "grade_status_or_unknown").orderBy(F.col("enrolled_at_date").asc_nulls_last(), "id")
w_all_by_enrolled_at_quarter = Window.orderBy("enrolled_at_quarter", "id")
w_student_id_grade_status_or_u = Window.partitionBy("student_id", "grade_status_or_unknown").orderBy(F.col("max_enrollment_score").desc(), "id")

prepared = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

without_matches = (
    prepared.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term") == "2024T2"), "left_anti")
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.max("score").alias("max_enrollment_score"),
    )
)

with_stats = (
    without_matches.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.max_enrollment_score",
    )
)

windowed = (
    with_stats
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "enrolled_at_quarter",
        "enrolled_at_date",
        "max_enrollment_score",
        F.row_number().over(w_course_id_grade_status_or_un).alias("seq_no"),
        F.row_number().over(w_all_by_enrolled_at_quarter).alias("seq_no_2"),
        F.lag("enrolled_at", 2).over(w_student_id_grade_status_or_u).alias("prev2_enrolled_at"),
        F.ntile(10).over(w_all_by_enrolled_at_quarter).alias("enrolled_at_quarter_decile"),
    )
)

result = (
    windowed
    .cube("term", "seq_no", "student_id")
    .agg(
        F.grouping("term").alias("is_total_term"),
        F.round(F.sum("max_enrollment_score"), 2).alias("total_max_enrollment_score"),
    )
    .orderBy(
        F.col("term").asc_nulls_last(),
        F.col("seq_no").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
