from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_enrollment_coun = Window.partitionBy("course_id").orderBy(F.col("enrollment_count").desc(), "id").rowsBetween(-2, Window.currentRow)
w_enrollment_count = Window.partitionBy("enrollment_count")
w_course_id_by_enrollment_coun_2 = Window.partitionBy("course_id").orderBy(F.col("enrollment_count").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_enrollment_enrolled_at = Window.partitionBy(F.to_date("last_enrollment_enrolled_at"), "grade_status_len").orderBy(F.col("score").desc(), "id").rowsBetween(-2, -1)
w_all_by_score_filled = Window.orderBy("score_filled", "id")

base_enrollments = (
    df_enrollments
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
        F.sum("score").alias("total_enrollment_score"),
    )
)

with_stats = (
    base_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.total_enrollment_score",
    )
)

windowed = (
    with_stats
    .select(
        "*",
        F.round(F.avg("grade_status_len").over(w_course_id_by_enrollment_coun), 2).alias("grade_status_len_avg_3"),
        F.size(F.collect_set("term").over(w_enrollment_count)).alias("distinct_term_in_enrollment_count"),
        F.round(F.col("enrollment_count") / F.nullif(F.sum("enrollment_count").over(w_enrollment_count), F.lit(0)), 4).alias("enrollment_count_share_of_enrollment_count"),
        F.nth_value("enrollment_count", 2).over(w_course_id_by_enrollment_coun_2).alias("second_enrollment_count"),
        F.max("enrollment_count").over(w_last_enrollment_enrolled_at).alias("enrollment_count_max_2_prior"),
        F.row_number().over(w_all_by_score_filled).alias("seq_no"),
    )
)

with_parent_student = (
    windowed.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select("s.*", F.col("st.is_international").alias("student_is_international"))
)

result = (
    with_parent_student
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "grade_status_len",
        "last_enrollment_enrolled_at",
        "enrollment_count",
        "total_enrollment_score",
        "grade_status_len_avg_3",
        "enrollment_count_share_of_enrollment_count",
        "second_enrollment_count",
        "seq_no",
        "student_is_international",
    )
    .orderBy("enrolled_at", "student_id")
)
