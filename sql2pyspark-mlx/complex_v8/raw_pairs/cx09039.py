from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_score_bucket = Window.partitionBy("course_id").orderBy("score_bucket", "id")
w_all_by_student_gpa_running = Window.orderBy(F.col("student_gpa").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status = Window.partitionBy("grade_status")
w_grade_status_course_id_by_sc = Window.partitionBy("grade_status", "course_id").orderBy(F.col("score_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_bucket_full = Window.orderBy("score_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_score_ties = Window.partitionBy("course_id").orderBy(F.col("score").asc_nulls_last())
w_grade_status_course_id_by_sc_2 = Window.partitionBy("grade_status", "course_id").orderBy(F.col("score_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_student_gpa = Window.orderBy(F.col("student_gpa").asc_nulls_last(), "id")

base_enrollments = (
    df_enrollments
    .filter(F.col("score") > 20.2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

combined_details_course = (
    base_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.score_bucket",
        "c.course_code",
        "st.student_name",
        F.col("st.gpa").alias("student_gpa"),
    )
)

sequenced = (
    combined_details_course
    .select(
        "*",
        F.ntile(10).over(w_course_id_by_score_bucket).alias("score_bucket_decile"),
        F.last("student_gpa", ignorenulls=True).over(w_all_by_student_gpa_running).alias("last_known_student_gpa"),
        F.max("student_gpa").over(w_grade_status).alias("grade_status_max_student_gpa"),
        F.min("score").over(w_grade_status_course_id_by_sc).alias("min_score_so_far"),
        F.nth_value("student_gpa", 2).over(w_all_by_score_bucket_full).alias("second_student_gpa"),
        F.dense_rank().over(w_course_id_by_score_ties).alias("score_dense_rank"),
        F.nth_value("score_bucket", 2).over(w_grade_status_course_id_by_sc_2).alias("second_score_bucket"),
        (F.col("student_gpa") - F.lag("student_gpa").over(w_all_by_student_gpa)).alias("student_gpa_change"),
        F.lead("score_bucket", 3, 0).over(w_all_by_student_gpa).alias("next3_score_bucket"),
    )
)

tagged = (
    sequenced
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "score_bucket",
        "course_code",
        "score_bucket_decile",
        "grade_status_max_student_gpa",
        "min_score_so_far",
        "score_dense_rank",
        "student_gpa_change",
        F.abs(F.col("score_bucket") - 65).alias("score_bucket_distance"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.initcap(F.trim(F.col("course_code"))).alias("course_code_clean"),
    )
)

result = (
    tagged
    .filter(F.col("student_id") <= 10)
    .select(
        "id",
        "score",
        "grade_status",
        "score_bucket",
        "course_code",
        "grade_status_max_student_gpa",
        "min_score_so_far",
        "score_dense_rank",
        "score_bucket_distance",
        "grade_status_or_unknown",
        "course_code_clean",
    )
    .orderBy(F.col("id").desc(), "score_bucket")
    .limit(5)
)
