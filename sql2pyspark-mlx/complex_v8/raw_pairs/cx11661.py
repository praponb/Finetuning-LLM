from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_all_by_enrolled_at_prior_6 = Window.orderBy("enrolled_at", "id").rowsBetween(-6, -1)
w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy(F.col("score").desc(), "id")
w_student_id_course_id_by_scor_2 = Window.partitionBy("student_id", "course_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_to_date_term = Window.partitionBy(F.to_date("enrolled_at"), "term")
w_student_id_course_id_by_scor_3 = Window.partitionBy("student_id", "course_id").orderBy(F.col("score").desc())

prepared_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

with_windows = (
    prepared_enrollments
    .select(
        "*",
        F.ntile(5).over(w_all_by_enrolled_at).alias("enrolled_at_quintile"),
        F.round(F.avg("score").over(w_all_by_enrolled_at_prior_6), 2).alias("score_avg_6_prior"),
        F.lead("score").over(w_student_id_course_id_by_scor).alias("next_score"),
        F.first("score").over(w_student_id_course_id_by_scor_2).alias("first_score"),
        F.max("score").over(w_enrolled_at_to_date_term).alias("enrolled_at_to_date_term_max_score"),
        F.rank().over(w_student_id_course_id_by_scor_3).alias("score_rank"),
        F.min("score").over(w_enrolled_at_to_date_term).alias("enrolled_at_to_date_term_min_score"),
        F.count(F.lit(1)).over(w_enrolled_at_to_date_term).alias("rows_in_enrolled_at_to_date_term"),
    )
)

result = (
    with_windows
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "enrolled_at",
        "enrolled_at_quintile",
        "score_avg_6_prior",
        "next_score",
        "first_score",
        "score_rank",
        "rows_in_enrolled_at_to_date_term",
        F.when(F.col("term").isin("2024T2", "2025T1"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
        (F.col("score") + F.col("enrolled_at_quintile")).alias("score_plus_enrolled_at_quintile"),
    )
    .orderBy(F.col("term_group").desc_nulls_first())
)
