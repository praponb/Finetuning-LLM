from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_student_id_by_enrolled_at_ru = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_term = Window.partitionBy("course_id", "term")
w_all_by_score_ties = Window.orderBy(F.col("score").desc())
w_enrolled_at_month_by_enrolle = Window.partitionBy(F.month("enrolled_at")).orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_month_by_enrolle_2 = Window.partitionBy(F.month("enrolled_at")).orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_student_id = Window.partitionBy("grade_status", "student_id")
w_score_quartile_by_score_shar = Window.partitionBy("score_quartile").orderBy("score_share_of_course_id_term", "id")
w_all_by_minutes_since_prev_en = Window.orderBy("minutes_since_prev_enrolled_at", "id")
w_last_known_enrolled_at_year = Window.partitionBy(F.year("last_known_enrolled_at"), "student_id")

scoped_enrollments = (
    df_enrollments
    .filter(~F.col("grade_status").isin("PASS", "INCOMPLETE"))
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

windowed = (
    scoped_enrollments
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrolled_at",
        F.round(F.percent_rank().over(w_all_by_score), 3).alias("score_percent_rank"),
        F.first("score", ignorenulls=True).over(w_student_id_by_enrolled_at_ru).alias("first_known_score"),
        F.size(F.collect_set("student_id").over(w_course_id_term)).alias("distinct_student_id_in_course_id_term"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id_term), F.lit(0)), 4).alias("score_share_of_course_id_term"),
        F.round(F.percent_rank().over(w_all_by_score_ties), 3).alias("score_percent_rank_2"),
        F.first("score").over(w_enrolled_at_month_by_enrolle).alias("first_score"),
        F.last("enrolled_at", ignorenulls=True).over(w_enrolled_at_month_by_enrolle_2).alias("last_known_enrolled_at"),
        F.sum("score").over(w_course_id_term).alias("course_id_term_sum_score"),
        F.ntile(4).over(w_all_by_score).alias("score_quartile"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_score))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.min("score").over(w_course_id_term).alias("course_id_term_min_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_student_id), F.lit(0)), 4).alias("score_share_of_grade_status_student_id"),
        F.count(F.lit(1)).over(w_grade_status_student_id).alias("rows_in_grade_status_student_id"),
    )
)

scored = (
    windowed
    .withColumn("minutes_last_known_enrolled_at_to_enrolled_at", F.round((F.unix_timestamp(F.col("enrolled_at")) - F.unix_timestamp(F.col("last_known_enrolled_at"))) / 60, 1))
)

metrics = (
    scored
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrolled_at",
        "first_known_score",
        "distinct_student_id_in_course_id_term",
        "score_share_of_course_id_term",
        "score_percent_rank_2",
        "first_score",
        "last_known_enrolled_at",
        "course_id_term_sum_score",
        "score_quartile",
        "course_id_term_min_score",
        "score_share_of_grade_status_student_id",
        "rows_in_grade_status_student_id",
        "minutes_last_known_enrolled_at_to_enrolled_at",
        F.round((F.unix_timestamp("last_known_enrolled_at") - F.unix_timestamp(F.lag("last_known_enrolled_at").over(w_score_quartile_by_score_shar))) / 60, 1).alias("minutes_since_prev_last_known_enrolled_at"),
        F.lag("first_known_score", 1, 0).over(w_all_by_minutes_since_prev_en).alias("prev_first_known_score"),
        F.lead("score_percent_rank").over(w_all_by_minutes_since_prev_en).alias("next_score_percent_rank"),
        F.count(F.lit(1)).over(w_last_known_enrolled_at_year).alias("rows_in_last_known_enrolled_at_year_student_id"),
        F.round(F.col("score_percent_rank") / F.nullif(F.sum("score_percent_rank").over(w_last_known_enrolled_at_year), F.lit(0)), 4).alias("score_percent_rank_share_of_last_known_enrolled_"),
        F.row_number().over(w_all_by_minutes_since_prev_en).alias("rn"),
    )
)

result = (
    metrics
    .select(
        "id",
        "grade_status",
        "first_known_score",
        "distinct_student_id_in_course_id_term",
        "first_score",
        "last_known_enrolled_at",
        "course_id_term_sum_score",
        "score_quartile",
        "score_share_of_grade_status_student_id",
        "rows_in_grade_status_student_id",
        "minutes_last_known_enrolled_at_to_enrolled_at",
        "minutes_since_prev_last_known_enrolled_at",
        "prev_first_known_score",
        "next_score_percent_rank",
        "score_percent_rank_share_of_last_known_enrolled_",
        "rn",
        F.coalesce(F.col("rn"), F.lit(0)).alias("rn_filled"),
    )
    .orderBy("rn_filled", F.col("prev_first_known_score").desc())
)
