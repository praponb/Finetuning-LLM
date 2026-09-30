from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_plus_7d_month_st = Window.partitionBy(F.month("enrolled_at_plus_7d"), "student_id").orderBy("score", "id")
w_student_id = Window.partitionBy("student_id")
w_enrolled_at_plus_7d_month_st_2 = Window.partitionBy(F.month("enrolled_at_plus_7d"), "student_id").orderBy("score")

recent = (
    df_enrollments
    .filter(F.col("student_id") >= 19)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

combined_details_course = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.upper(F.trim(F.col("c.course_code"))).like("C%"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_filled",
        "s.grade_status_slug",
        "c.course_code",
    )
)

derived = (
    combined_details_course
    .select(
        "*",
        (F.col("enrolled_at") + F.expr("INTERVAL 7 DAYS")).alias("enrolled_at_plus_7d"),
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
    )
)

windowed = (
    derived
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_filled",
        "grade_status_slug",
        "enrolled_at_year",
        "term_ref",
        F.round(F.cume_dist().over(w_enrolled_at_plus_7d_month_st), 3).alias("score_cume_dist"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.min("score").over(w_student_id).alias("student_id_min_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.round(F.col("score") - F.avg("score").over(w_student_id), 2).alias("score_vs_student_id_avg"),
        F.dense_rank().over(w_enrolled_at_plus_7d_month_st_2).alias("score_dense_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "grade_status_slug",
        "enrolled_at_year",
        "term_ref",
        "score_cume_dist",
        "student_id_min_score",
        "score_share_of_student_id",
        "score_vs_student_id_avg",
        "score_dense_rank",
        F.coalesce(F.col("score_dense_rank"), F.lit(0)).alias("score_dense_rank_filled"),
    )
    .orderBy(F.col("enrolled_at").desc())
)
