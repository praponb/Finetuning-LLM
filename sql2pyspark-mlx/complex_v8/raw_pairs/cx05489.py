from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_student_id = Window.partitionBy("grade_status", "student_id")
w_all_by_student_gpa_full = Window.orderBy("student_gpa", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_student_id_by_grade_status_s = Window.partitionBy("student_id").orderBy(F.col("grade_status_student_id_avg_student_gpa").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_student_id_by_s = Window.partitionBy("grade_status", "student_id").orderBy("student_id_grade_status_max_student_gpa", "id")
w_course_id_grade_status = Window.partitionBy("course_id", "grade_status")
w_student_id_by_grade_status_s_2 = Window.partitionBy("student_id").orderBy(F.col("grade_status_student_id_avg_student_gpa").asc_nulls_last(), "id")

valid_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-19 14:00:00").cast("timestamp")) | (F.col("term") == "2024T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat_ws(": ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

combined_details = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.grade_status_term_label",
        "s.grade_status_slug",
        "st.student_name",
        F.col("st.gpa").alias("student_gpa"),
    )
)

with_trends = (
    combined_details
    .select(
        "*",
        F.round(F.col("student_gpa") / F.nullif(F.sum("student_gpa").over(w_grade_status_student_id), F.lit(0)), 4).alias("student_gpa_share_of_grade_status_student_id"),
        F.max("score").over(w_grade_status_student_id).alias("grade_status_student_id_max_score"),
        F.round(F.avg("student_gpa").over(w_grade_status_student_id), 2).alias("grade_status_student_id_avg_student_gpa"),
        F.last("student_gpa").over(w_all_by_student_gpa_full).alias("last_student_gpa"),
        F.round(F.avg("score").over(w_student_id_grade_status), 2).alias("student_id_grade_status_avg_score"),
        F.round(F.col("student_gpa") / F.nullif(F.sum("student_gpa").over(w_student_id_grade_status), F.lit(0)), 4).alias("student_gpa_share_of_student_id_grade_status"),
        F.max("student_gpa").over(w_student_id_grade_status).alias("student_id_grade_status_max_student_gpa"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status_term_label",
        "student_name",
        "student_gpa",
        "student_gpa_share_of_grade_status_student_id",
        "last_student_gpa",
        "student_gpa_share_of_student_id_grade_status",
        F.count(F.lit(1)).over(w_student_id_by_grade_status_s).alias("running_count"),
        F.ntile(5).over(w_grade_status_student_id_by_s).alias("student_id_grade_status_max_student_gpa_quintile"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id_grade_status), F.lit(0)), 4).alias("score_share_of_course_id_grade_status"),
        F.dense_rank().over(w_student_id_by_grade_status_s_2).alias("grade_status_student_id_avg_student_gpa_dense_ra"),
        F.lag("student_gpa", 2).over(w_grade_status_student_id_by_s).alias("prev2_student_gpa"),
        F.count(F.lit(1)).over(w_course_id_grade_status).alias("rows_in_course_id_grade_status"),
        F.round(F.avg("score").over(w_course_id_grade_status), 2).alias("course_id_grade_status_avg_score"),
    )
)

summary = (
    metrics
    .filter(F.col("grade_status_term_label").isNotNull())
    .groupBy("student_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.sum("score"), 2).alias("total_score"),
        F.array_sort(F.collect_set("student_name")).alias("student_name_list"),
    )
)

result = (
    summary
    .filter(F.size(F.col("student_name_list")) >= 1)
    .select(
        "student_id",
        "n_rows",
        "total_score",
        "student_name_list",
    )
    .orderBy(F.col("total_score").desc_nulls_first(), "student_id")
    .limit(10)
)
