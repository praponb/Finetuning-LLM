from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_enrolled_at_mon = Window.partitionBy("grade_status", F.month("enrolled_at"))
w_course_id_by_score = Window.partitionBy("course_id").orderBy(F.col("score").desc(), "id")
w_grade_status_by_score_bucket = Window.partitionBy("grade_status").orderBy("score_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_by_score_ties = Window.partitionBy("course_id").orderBy(F.col("score").desc())
w_grade_status_by_score_bucket_2 = Window.partitionBy("grade_status").orderBy("score_bucket", "id")
w_all_by_score_bucket = Window.orderBy("score_bucket", "id")
w_grade_status_by_score_bucket_3 = Window.partitionBy("grade_status").orderBy("score_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_by_score_bucket_4 = Window.partitionBy("grade_status").orderBy("score_bucket", "id").rowsBetween(-3, -1)

valid = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-03-08 11:00:00").cast("timestamp")) | (F.col("course_id") <= 7))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

ranked = (
    valid
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_bucket",
        F.min("score_bucket").over(w_grade_status_enrolled_at_mon).alias("grade_status_enrolled_at_month_min_score_bucket"),
        F.round(F.percent_rank().over(w_course_id_by_score), 3).alias("score_percent_rank"),
        F.first("score", ignorenulls=True).over(w_grade_status_by_score_bucket).alias("first_known_score"),
        F.round(F.cume_dist().over(w_course_id_by_score_ties), 3).alias("score_cume_dist"),
        F.lead("score_bucket", 1, 0).over(w_grade_status_by_score_bucket_2).alias("next_score_bucket"),
        F.count(F.lit(1)).over(w_grade_status_enrolled_at_mon).alias("rows_in_grade_status_enrolled_at_month"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_enrolled_at_mon), F.lit(0)), 4).alias("score_share_of_grade_status_enrolled_at_month"),
        F.row_number().over(w_all_by_score_bucket).alias("rn"),
        F.first("score_bucket").over(w_grade_status_by_score_bucket_3).alias("first_score_bucket"),
        F.round(F.avg("score_bucket").over(w_grade_status_enrolled_at_mon), 2).alias("grade_status_enrolled_at_month_avg_score_bucket"),
        F.round(F.avg("score").over(w_grade_status_by_score_bucket_4), 2).alias("score_avg_3_prior"),
        F.max("score_bucket").over(w_grade_status_enrolled_at_mon).alias("grade_status_enrolled_at_month_max_score_bucket"),
        F.round(F.col("score_bucket") / F.nullif(F.sum("score_bucket").over(w_grade_status_enrolled_at_mon), F.lit(0)), 4).alias("score_bucket_share_of_grade_status_enrolled_at_m"),
    )
)

leaders = (
    ranked
    .filter((F.col("rn") <= 2) & (F.col("score_share_of_grade_status_enrolled_at_month") < 0.7))
    .drop("rn")
)

result = (
    leaders
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_percent_rank",
        "first_known_score",
        "next_score_bucket",
        "rows_in_grade_status_enrolled_at_month",
        "score_share_of_grade_status_enrolled_at_month",
        "first_score_bucket",
        "grade_status_enrolled_at_month_avg_score_bucket",
        "score_bucket_share_of_grade_status_enrolled_at_m",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
    .orderBy(F.col("grade_status_slug").desc())
)
