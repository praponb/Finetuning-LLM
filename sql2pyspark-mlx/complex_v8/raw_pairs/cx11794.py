from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_student_id_by_enrolled = Window.partitionBy("term", "student_id").orderBy("enrolled_at", "id").rowsBetween(-6, Window.currentRow)
w_enrolled_at_month_student_id = Window.partitionBy(F.month("enrolled_at"), "student_id")

filtered_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isin("INCOMPLETE", "PASS")) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

ranked = (
    filtered_enrollments
    .select(
        "*",
        F.sum("score").over(w_term_student_id_by_enrolled).alias("score_sum_7"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_month_student_id), F.lit(0)), 4).alias("score_share_of_enrolled_at_month_student_id"),
    )
)

annotated = (
    ranked
    .select(
        "*",
        F.date_trunc("week", F.col("enrolled_at_month")).alias("enrolled_at_month_week"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.least(F.col("score"), F.col("score_sum_7")).alias("least_score_score_sum_7"),
    )
)

result = (
    annotated
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at_month",
        "score_sum_7",
        "score_share_of_enrolled_at_month_student_id",
        "enrolled_at_month_week",
        "term_len",
        "least_score_score_sum_7",
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
    )
)
