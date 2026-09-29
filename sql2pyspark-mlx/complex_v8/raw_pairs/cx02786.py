from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_course_id = Window.partitionBy("student_id", "course_id")
w_student_id = Window.partitionBy("student_id")
w_enrolled_at_to_date_by_score = Window.partitionBy(F.to_date("enrolled_at")).orderBy(F.col("score").desc())
w_enrolled_at_to_date_by_score_2 = Window.partitionBy(F.to_date("enrolled_at")).orderBy(F.col("score").desc(), "id")
w_all_by_score_running = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy("score", "id").rowsBetween(-6, Window.currentRow)
w_enrolled_at_to_date_by_score_3 = Window.partitionBy(F.to_date("enrolled_at")).orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_enrolled_at_pr = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(-1, -1)
w_student_id_by_enrolled_at_qu = Window.partitionBy("student_id").orderBy("enrolled_at_quintile", "id")
w_course_id_by_student_id_avg = Window.partitionBy("course_id").orderBy("student_id_avg_score", "id").rowsBetween(-2, Window.currentRow)
w_student_id_by_enrolled_at_qu_2 = Window.partitionBy("student_id").orderBy("enrolled_at_quintile")
w_student_id_by_enrolled_at_qu_3 = Window.partitionBy("student_id").orderBy("enrolled_at_quintile", "id").rowsBetween(-6, -1)
w_course_id_by_student_id_avg_2 = Window.partitionBy("course_id").orderBy("student_id_avg_score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_enrollments
    .filter((F.col("score") >= 38.3) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_course_id), F.lit(0)), 4).alias("score_share_of_student_id_course_id"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.round(F.percent_rank().over(w_enrolled_at_to_date_by_score), 3).alias("score_percent_rank"),
        F.lead("score").over(w_enrolled_at_to_date_by_score_2).alias("next_score"),
        F.max("score").over(w_all_by_score_running).alias("max_score_so_far"),
        F.lag("score").over(w_enrolled_at_to_date_by_score_2).alias("prev_score"),
        F.min("score").over(w_student_id_course_id).alias("student_id_course_id_min_score"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.round(F.cume_dist().over(w_enrolled_at_to_date_by_score_2), 3).alias("score_cume_dist"),
        F.round(F.avg("score").over(w_student_id), 2).alias("student_id_avg_score"),
        F.ntile(5).over(w_student_id_by_enrolled_at).alias("enrolled_at_quintile"),
        F.round(F.avg("score").over(w_student_id_course_id_by_scor), 2).alias("score_avg_7"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_id_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.sum("score").over(w_enrolled_at_to_date_by_score_3).alias("running_score"),
        F.round(F.avg("score").over(w_student_id_by_enrolled_at_pr), 2).alias("score_avg_1_prior"),
        F.count(F.lit(1)).over(w_student_id_course_id).alias("rows_in_student_id_course_id"),
    )
)

scored = (
    with_trends
    .filter(F.col("next_score") > 10.8)
    .select(
        "*",
        F.lower(F.trim(F.col("grade_status_slug"))).alias("grade_status_slug_clean"),
        F.concat(F.col("grade_status_slug"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_slug_ref"),
        F.coalesce(F.col("prev_score"), F.lit(0.0)).alias("prev_score_filled"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.rank().over(w_student_id_by_enrolled_at_qu).alias("enrolled_at_quintile_rank"),
        F.sum("score_percent_rank").over(w_course_id_by_student_id_avg).alias("score_percent_rank_sum_3"),
        F.dense_rank().over(w_student_id_by_enrolled_at_qu_2).alias("enrolled_at_quintile_dense_rank"),
        F.sum("prev_score_filled").over(w_student_id_by_enrolled_at_qu_3).alias("prev_score_filled_sum_6_prior"),
        F.nth_value("prev_score_filled", 2).over(w_course_id_by_student_id_avg_2).alias("second_prev_score_filled"),
        F.rank().over(w_student_id_by_enrolled_at_qu_2).alias("enrolled_at_quintile_rank_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "grade_status_slug",
        "score_share_of_student_id_course_id",
        "score_percent_rank",
        "next_score",
        "max_score_so_far",
        "prev_score",
        "student_id_course_id_min_score",
        "rows_in_student_id",
        "score_cume_dist",
        "student_id_avg_score",
        "score_avg_7",
        "minutes_since_prev_enrolled_at",
        "running_score",
        "grade_status_slug_clean",
        "prev_score_filled",
        "score_percent_rank_sum_3",
        "second_prev_score_filled",
        "enrolled_at_quintile_rank_2",
    )
)
