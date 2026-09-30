from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_enrollment_term_by_stud = Window.partitionBy("term", "enrollment_term").orderBy(F.col("student_gpa").desc(), "id", "enrollment_id")
w_student_enrolled_on_month_by = Window.partitionBy(F.month("student_enrolled_on")).orderBy("enrollment_grade_status_count", "id", "enrollment_id")
w_grade_status_or_unknown = Window.partitionBy("grade_status_or_unknown")
w_student_enrolled_on_month_by_2 = Window.partitionBy(F.month("student_enrolled_on")).orderBy("enrollment_grade_status_count", "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_enrollment_term_by_stud_2 = Window.partitionBy("term", "enrollment_term").orderBy(F.col("student_gpa").desc())

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("score") <= 74.9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

exploded_detail_enrollments = (
    cleaned_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.grade_status_or_unknown",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

joined = (
    exploded_detail_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.year(F.col("st.enrolled_on")) == 2024)
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.grade_status_or_unknown",
        "s.enrollment_id",
        "s.enrollment_term",
        "s.enrollment_grade_status",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.sum("score").alias("total_enrollment_score"),
        F.count(F.when(~F.col("grade_status").isin("PASS", "WITHDRAWN"), 1)).alias("enrollment_grade_status_count"),
    )
)

with_stats = (
    joined.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.total_enrollment_score",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

windowed = (
    with_stats
    .select(
        "id",
        "course_id",
        "term",
        "grade_status_or_unknown",
        "enrollment_id",
        "enrollment_term",
        "enrollment_grade_status",
        "student_enrolled_on",
        "enrollment_grade_status_count",
        F.round(F.percent_rank().over(w_term_enrollment_term_by_stud), 3).alias("student_gpa_percent_rank"),
        F.lead("student_gpa", 2).over(w_student_enrolled_on_month_by).alias("next2_student_gpa"),
        F.count(F.lit(1)).over(w_grade_status_or_unknown).alias("rows_in_grade_status_or_unknown"),
        F.sum("student_gpa").over(w_student_enrolled_on_month_by_2).alias("running_student_gpa"),
        F.round(F.percent_rank().over(w_term_enrollment_term_by_stud_2), 3).alias("student_gpa_percent_rank_2"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_grade_status_or_unknown), 2).alias("student_gpa_vs_grade_status_or_unknown_avg"),
    )
)

scored = (
    windowed
    .filter(F.col("student_gpa_percent_rank") < 0.7)
    .select(
        "id",
        "course_id",
        "term",
        "grade_status_or_unknown",
        "enrollment_id",
        "enrollment_grade_status",
        "student_enrolled_on",
        "student_gpa_percent_rank",
        "rows_in_grade_status_or_unknown",
        "running_student_gpa",
        "student_gpa_percent_rank_2",
        "student_gpa_vs_grade_status_or_unknown_avg",
        F.concat_ws("-", F.col("enrollment_grade_status"), F.col("enrollment_term")).alias("enrollment_grade_status_enrollment_term_label"),
    )
)

result = (
    scored
    .select(
        "id",
        "enrollment_id",
        "student_gpa_percent_rank",
        "student_gpa_percent_rank_2",
        "student_gpa_vs_grade_status_or_unknown_avg",
    )
    .unpivot(
        ids=["id", "enrollment_id"],
        values=["student_gpa_percent_rank", "student_gpa_percent_rank_2", "student_gpa_vs_grade_status_or_unknown_avg"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
