from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id")
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_term = Window.partitionBy("student_id", "term")
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy("score", "id")
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_all_by_score_vs_student_id_t = Window.orderBy(F.col("score_vs_student_id_term_avg").desc())

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-02-24 13:00:00").cast("timestamp")) & (F.col("grade_status") == "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
    )
)

filtered = (
    cleaned_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
)

sequenced = (
    filtered
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "grade_status_slug",
        "term_prefix",
        "term_clean",
        F.ntile(4).over(w_grade_status_by_enrolled_at).alias("enrolled_at_quartile"),
        F.first("score", ignorenulls=True).over(w_grade_status_by_enrolled_at_2).alias("first_known_score"),
        F.round(F.col("score") - F.avg("score").over(w_student_id_term), 2).alias("score_vs_student_id_term_avg"),
        F.round(F.cume_dist().over(w_grade_status_by_score), 3).alias("score_cume_dist"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.count(F.lit(1)).over(w_grade_status_course_id).alias("rows_in_grade_status_course_id"),
        F.min("enrolled_at_quartile").over(w_grade_status_course_id).alias("grade_status_course_id_min_enrolled_at_quartile"),
        F.round(F.col("score_vs_student_id_term_avg") - F.avg("score_vs_student_id_term_avg").over(w_grade_status_course_id), 2).alias("score_vs_student_id_term_avg_vs_grade_status_cou"),
        F.rank().over(w_all_by_score_vs_student_id_t).alias("score_vs_student_id_term_avg_rank"),
        F.size(F.collect_set("student_id").over(w_grade_status_course_id)).alias("distinct_student_id_in_grade_status_course_id"),
    )
)

result = (
    sequenced_2
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "grade_status_slug",
        "term_prefix",
        "term_clean",
        "first_known_score",
        "score_vs_student_id_term_avg",
        "score_cume_dist",
        "rows_in_grade_status_course_id",
        "score_vs_student_id_term_avg_vs_grade_status_cou",
        "score_vs_student_id_term_avg_rank",
        "distinct_student_id_in_grade_status_course_id",
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
        F.when(F.col("score_cume_dist") >= 0.8, "A").when(F.col("score_cume_dist") >= 0.3, "B").otherwise("C").alias("score_cume_dist_tier"),
    )
    .orderBy(F.col("student_id").asc(), F.col("term_clean").desc())
)
