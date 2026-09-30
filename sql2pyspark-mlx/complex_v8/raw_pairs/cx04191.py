from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_enrolled_at_ru = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").desc_nulls_first())
w_enrolled_at_tercile_by_n_ter = Window.partitionBy("enrolled_at_tercile").orderBy(F.col("n_term_is_not_null").asc_nulls_last(), "enrolled_at_tercile", "first_known_score_tier")
w_all_by_distinct_grade_status = Window.orderBy("distinct_grade_status", "enrolled_at_tercile", "first_known_score_tier")

base = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

ranked = (
    base
    .select(
        "*",
        F.first("score", ignorenulls=True).over(w_student_id_by_enrolled_at_ru).alias("first_known_score"),
        F.ntile(5).over(w_all_by_enrolled_at).alias("enrolled_at_quintile"),
        F.lead("score").over(w_student_id_by_enrolled_at).alias("next_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_course_id), F.lit(0)), 4).alias("score_share_of_grade_status_course_id"),
        F.ntile(3).over(w_course_id_by_enrolled_at).alias("enrolled_at_tercile"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_percent_rank"),
        F.round(F.avg("score").over(w_grade_status_course_id), 2).alias("grade_status_course_id_avg_score"),
        F.lag("score", 2, 0).over(w_all_by_enrolled_at).alias("prev2_score"),
        F.sum("score").over(w_grade_status_course_id).alias("grade_status_course_id_sum_score"),
    )
)

per_group = (
    ranked
    .filter(F.col("score_share_of_grade_status_course_id") > 0.1)
    .groupBy(
        "enrolled_at_tercile",
        F.when(F.col("first_known_score") >= 79.2, "A").when(F.col("first_known_score") >= 38.1, "B").otherwise("C").alias("first_known_score_tier"),
    )
    .agg(
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.sum("enrolled_at_quintile").alias("total_enrolled_at_quintile"),
        F.count(F.when(F.col("term").isNotNull(), 1)).alias("n_term_is_not_null"),
    )
)

ranked_2 = (
    per_group
    .select(
        "*",
        F.row_number().over(w_enrolled_at_tercile_by_n_ter).alias("n_term_is_not_null_seq"),
        F.rank().over(w_all_by_distinct_grade_status).alias("distinct_grade_status_rank"),
    )
)

result = (
    ranked_2
    .select(
        "enrolled_at_tercile",
        "first_known_score_tier",
        "total_enrolled_at_quintile",
        "n_term_is_not_null",
        "n_term_is_not_null_seq",
        "distinct_grade_status_rank",
        F.when(F.col("distinct_grade_status") >= 8, "HIGH").when(F.col("distinct_grade_status") >= 5, "MEDIUM").otherwise("LOW").alias("distinct_grade_status_tier"),
        F.coalesce(F.col("first_known_score_tier"), F.lit("UNKNOWN")).alias("first_known_score_tier_or_unknown"),
    )
    .orderBy(F.col("enrolled_at_tercile").desc(), "first_known_score_tier")
)
