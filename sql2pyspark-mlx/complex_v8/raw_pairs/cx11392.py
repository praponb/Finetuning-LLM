from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_student_id = Window.partitionBy("grade_status", "student_id")
w_student_id_by_score_full = Window.partitionBy("student_id").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 44.4).alias("score_distance"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
    )
)

qualified = (
    base
    .crossJoin(base.agg(F.min("score_distance").alias("overall_min_score_distance")))
    .filter(F.col("score_distance") > F.col("overall_min_score_distance"))
    .drop("overall_min_score_distance")
)

windowed = (
    qualified
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "score_distance",
        "grade_status_ref",
        F.size(F.collect_set("course_id").over(w_grade_status_student_id)).alias("distinct_course_id_in_grade_status_student_id"),
        F.last("enrolled_at").over(w_student_id_by_score_full).alias("last_enrolled_at"),
        F.round(F.col("score_distance") / F.nullif(F.sum("score_distance").over(w_grade_status_student_id), F.lit(0)), 4).alias("score_distance_share_of_grade_status_student_id"),
    )
)

joined_student = (
    windowed.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.cohort") == "C2024")
    .select(
        "s.id",
        "s.student_id",
        "s.term",
        "s.score",
        "s.score_distance",
        "s.grade_status_ref",
        "s.distinct_course_id_in_grade_status_student_id",
        "s.last_enrolled_at",
        "s.score_distance_share_of_grade_status_student_id",
        F.col("st.cohort").alias("student_cohort"),
    )
)

derived = (
    joined_student
    .select(
        "*",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.date_trunc("quarter", F.col("last_enrolled_at")).alias("last_enrolled_at_quarter"),
        F.concat_ws("|", F.col("grade_status_ref"), F.col("student_cohort")).alias("grade_status_ref_student_cohort_label"),
    )
)

stats = (
    derived
    .groupBy("distinct_course_id_in_grade_status_student_id", "student_id")
    .agg(
        F.max("score_distance").alias("max_score_distance"),
        F.count(F.when(F.col("student_cohort") == "C2022", 1)).alias("n_student_cohort_c2022"),
        F.sum("distinct_course_id_in_grade_status_student_id").alias("total_distinct_course_id_in_grade_status_student"),
        F.avg("score_distance_share_of_grade_status_student_id").alias("avg_score_distance_share_of_grade_status_student"),
    )
    .filter(F.col("avg_score_distance_share_of_grade_status_student") > 0.2)
    .drop("avg_score_distance_share_of_grade_status_student")
)

result = (
    stats
    .select(
        "distinct_course_id_in_grade_status_student_id",
        "n_student_cohort_c2022",
        "total_distinct_course_id_in_grade_status_student",
        F.when(F.col("n_student_cohort_c2022") >= 6, "A").when(F.col("n_student_cohort_c2022") >= 4, "B").otherwise("C").alias("n_student_cohort_c2022_tier"),
    )
    .orderBy(F.col("n_student_cohort_c2022_tier").desc())
)
