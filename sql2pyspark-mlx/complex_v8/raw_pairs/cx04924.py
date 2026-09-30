from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_level_by_enrolled_at = Window.partitionBy("course_level").orderBy("enrolled_at")
w_course_level_by_enrolled_at_2 = Window.partitionBy("course_level").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_credits_student_id_by = Window.partitionBy("course_credits", "student_id").orderBy("days_since_enrolled_at", "id")
w_term_course_credits_by_score = Window.partitionBy("term", "course_credits").orderBy("score", "id")
w_course_credits_student_id_by_2 = Window.partitionBy("course_credits", "student_id").orderBy("days_since_enrolled_at")
w_student_id_course_id = Window.partitionBy("student_id", "course_id")
w_all_by_score_full = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_credits_student_id = Window.partitionBy("course_credits", "student_id")
w_days_since_enrolled_at_decil = Window.partitionBy("days_since_enrolled_at_decile").orderBy(F.col("rn").desc(), "id")
w_prev_course_credits_by_first = Window.partitionBy("prev_course_credits").orderBy(F.col("first_score").desc(), "id").rowsBetween(-2, -1)

valid = (
    df_enrollments
    .filter((F.col("grade_status") == "FAIL") & (F.col("score") < 78.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
    )
)

with_parent_course = (
    valid.alias("s")
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
        "s.days_since_enrolled_at",
        "s.grade_status_ref",
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
        F.col("c.level").alias("course_level"),
    )
)

sequenced = (
    with_parent_course
    .select(
        "*",
        F.round(F.cume_dist().over(w_course_level_by_enrolled_at), 3).alias("enrolled_at_cume_dist"),
        F.first("score").over(w_course_level_by_enrolled_at_2).alias("first_score"),
        F.ntile(10).over(w_course_credits_student_id_by).alias("days_since_enrolled_at_decile"),
        F.lag("course_credits").over(w_term_course_credits_by_score).alias("prev_course_credits"),
        F.row_number().over(w_course_credits_student_id_by).alias("rn"),
        F.dense_rank().over(w_course_credits_student_id_by_2).alias("days_since_enrolled_at_dense_rank"),
        F.round(F.col("days_since_enrolled_at") / F.nullif(F.sum("days_since_enrolled_at").over(w_student_id_course_id), F.lit(0)), 4).alias("days_since_enrolled_at_share_of_student_id_cours"),
        F.first("score").over(w_all_by_score_full).alias("first_score_2"),
        F.round(F.col("score") - F.avg("score").over(w_student_id_course_id), 2).alias("score_vs_student_id_course_id_avg"),
    )
)

leaders = sequenced.filter(F.col("days_since_enrolled_at_dense_rank") <= 3)

metrics = (
    leaders
    .select(
        "*",
        F.min("prev_course_credits").over(w_course_credits_student_id).alias("course_credits_student_id_min_prev_course_credit"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_credits_student_id), F.lit(0)), 4).alias("score_share_of_course_credits_student_id"),
        F.lead("rn", 3).over(w_days_since_enrolled_at_decil).alias("next3_rn"),
        F.round(F.avg("first_score_2").over(w_prev_course_credits_by_first), 2).alias("first_score_2_avg_2_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "days_since_enrolled_at",
        "course_credits",
        "course_code",
        "course_level",
        "enrolled_at_cume_dist",
        "first_score",
        "days_since_enrolled_at_decile",
        "rn",
        "days_since_enrolled_at_dense_rank",
        "first_score_2",
        "score_vs_student_id_course_id_avg",
        "score_share_of_course_credits_student_id",
        "next3_rn",
        "first_score_2_avg_2_prior",
    )
)
