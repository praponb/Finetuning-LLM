from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_score = Window.partitionBy("student_id").orderBy("score", "id")
w_enrolled_at_to_date = Window.partitionBy(F.to_date("enrolled_at"))
w_student_id_by_score_full = Window.partitionBy("student_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_course_id = Window.partitionBy("student_id", "course_id")
w_score_decile_term_by_score_d = Window.partitionBy("score_decile", "term").orderBy(F.col("score_decile").desc(), "id")

filtered = (
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
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.round(F.percent_rank().over(w_student_id_by_score), 3).alias("score_percent_rank"),
        F.min("score").over(w_enrolled_at_to_date).alias("enrolled_at_to_date_min_score"),
        F.nth_value("score", 2).over(w_student_id_by_score_full).alias("second_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_to_date), F.lit(0)), 4).alias("score_share_of_enrolled_at_to_date"),
        F.ntile(10).over(w_student_id_by_score).alias("score_decile"),
        F.last("enrolled_at", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("last_known_enrolled_at"),
        F.count(F.lit(1)).over(w_enrolled_at_to_date).alias("rows_in_enrolled_at_to_date"),
        F.first("enrolled_at").over(w_all_by_enrolled_at_full).alias("first_enrolled_at"),
        F.ntile(4).over(w_student_id_by_score).alias("score_quartile"),
    )
)

enriched = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "score_percent_rank",
        "enrolled_at_to_date_min_score",
        "second_score",
        "score_share_of_enrolled_at_to_date",
        "score_decile",
        "last_known_enrolled_at",
        "rows_in_enrolled_at_to_date",
        "first_enrolled_at",
        "score_quartile",
        (F.col("score_percent_rank") + F.col("second_score")).alias("score_percent_rank_plus_second_score"),
        F.date_trunc("month", F.col("first_enrolled_at")).alias("first_enrolled_at_month"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "score_percent_rank",
        "enrolled_at_to_date_min_score",
        "second_score",
        "score_quartile",
        "score_percent_rank_plus_second_score",
        F.max("rows_in_enrolled_at_to_date").over(w_student_id_course_id).alias("student_id_course_id_max_rows_in_enrolled_at_to_"),
        F.round(F.avg("second_score").over(w_student_id_course_id), 2).alias("student_id_course_id_avg_second_score"),
        F.lag("score_decile", 2).over(w_score_decile_term_by_score_d).alias("prev2_score_decile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "student_id",
        "score_percent_rank",
        "enrolled_at_to_date_min_score",
        "score_percent_rank_plus_second_score",
        "student_id_course_id_max_rows_in_enrolled_at_to_",
        F.when(F.col("score_percent_rank") >= 0.8, "large").when(F.col("score_percent_rank") >= 0.3, "medium").otherwise("small").alias("score_percent_rank_tier"),
        F.abs(F.col("score_quartile") - 4).alias("score_quartile_distance"),
    )
)
