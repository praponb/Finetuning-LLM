from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy("score", "id")
w_term = Window.partitionBy("term")
w_grade_status_term_by_score_m = Window.partitionBy("grade_status", "term").orderBy(F.col("score").desc(), "id").rowsBetween(-2, Window.currentRow)
w_grade_status_student_id = Window.partitionBy("grade_status", "student_id")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_term_grade_status_by_score_r = Window.partitionBy("term", "grade_status").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_moving_1 = Window.orderBy("score", "id").rowsBetween(-1, Window.currentRow)
w_enrolled_at_month_by_enrolle = Window.partitionBy(F.month("enrolled_at")).orderBy("enrolled_at", "id")
w_enrolled_at_month_by_enrolle_2 = Window.partitionBy(F.month("enrolled_at")).orderBy("enrolled_at")
w_all_by_score_moving_3 = Window.orderBy("score", "id").rowsBetween(-3, Window.currentRow)
w_grade_status_term_by_score = Window.partitionBy("grade_status", "term").orderBy(F.col("score").desc(), "id")

scoped_enrollments = (
    df_enrollments
    .filter((F.col("course_id") <= 6) & (F.hour(F.col("enrolled_at")).between(10, 15)))
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

with_trends = (
    scoped_enrollments
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.lead("score").over(w_all_by_score).alias("next_score"),
        F.min("score").over(w_term).alias("term_min_score"),
        F.round(F.avg("score").over(w_grade_status_term_by_score_m), 2).alias("score_avg_3"),
        F.max("score").over(w_grade_status_student_id).alias("grade_status_student_id_max_score"),
        F.round(F.avg("score").over(w_grade_status_student_id), 2).alias("grade_status_student_id_avg_score"),
        F.ntile(5).over(w_all_by_enrolled_at).alias("enrolled_at_quintile"),
        F.sum("score").over(w_grade_status_student_id).alias("grade_status_student_id_sum_score"),
        F.first("enrolled_at", ignorenulls=True).over(w_term_grade_status_by_score_r).alias("first_known_enrolled_at"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_student_id), 2).alias("score_vs_grade_status_student_id_avg"),
        F.sum("score").over(w_all_by_score_moving_1).alias("score_sum_2"),
        F.ntile(4).over(w_enrolled_at_month_by_enrolle).alias("enrolled_at_quartile"),
        F.row_number().over(w_all_by_enrolled_at).alias("seq_no"),
        F.dense_rank().over(w_enrolled_at_month_by_enrolle_2).alias("enrolled_at_dense_rank"),
        F.sum("score").over(w_all_by_score_moving_3).alias("score_sum_4"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_grade_status_term_by_score))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
    )
)

enriched = (
    with_trends
    .filter(F.col("grade_status").isNotNull())
    .select(
        "*",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
        F.round((F.unix_timestamp(F.col("enrolled_at")) - F.unix_timestamp(F.col("first_known_enrolled_at"))) / 60, 1).alias("minutes_first_known_enrolled_at_to_enrolled_at"),
    )
)

result = (
    enriched
    .filter(F.col("score_sum_2").between(30.0, 210.0))
    .select("enrolled_at_quartile", "score_sum_2", "id")
    .distinct()
    .orderBy("id")
)
