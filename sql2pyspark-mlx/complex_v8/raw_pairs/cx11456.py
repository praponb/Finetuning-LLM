from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_score_tier_by_enrolled_at_fu = Window.partitionBy("score_tier").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_year_grade_statu = Window.partitionBy(F.year("enrolled_at"), "grade_status").orderBy("score", "id")
w_student_id = Window.partitionBy("student_id")

base_enrollments = (
    df_enrollments
    .filter((F.col("term") == "2025T1") & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
        F.when(F.col("score") >= 57.6, "A").when(F.col("score") >= 20.2, "B").otherwise("C").alias("score_tier"),
        F.abs(F.col("score") - 42.4).alias("score_distance"),
    )
)

sequenced = (
    base_enrollments
    .select(
        "*",
        F.last("score").over(w_score_tier_by_enrolled_at_fu).alias("last_score"),
        F.lead("score_distance", 2).over(w_enrolled_at_year_grade_statu).alias("next2_score_distance"),
        F.min("score").over(w_student_id).alias("student_id_min_score"),
        F.round(F.col("score_distance") / F.nullif(F.sum("score_distance").over(w_student_id), F.lit(0)), 4).alias("score_distance_share_of_student_id"),
    )
)

enriched = (
    sequenced
    .filter(F.col("last_score") > 38.3)
    .select(
        "*",
        F.round(F.col("score_distance") * 0.9, 2).alias("score_distance_adj"),
    )
)

result = (
    enriched
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "term_prefix",
        "score_tier",
        "score_distance",
        "last_score",
        "next2_score_distance",
        "student_id_min_score",
        "score_distance_share_of_student_id",
        "score_distance_adj",
    )
    .orderBy(F.col("student_id").desc(), "next2_score_distance")
)
