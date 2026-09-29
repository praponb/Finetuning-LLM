from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_all_by_enrolled_at_prior_1 = Window.orderBy("enrolled_at", "id").rowsBetween(-1, -1)
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_course_level_by_score = Window.partitionBy("course_level").orderBy(F.col("score").asc_nulls_last(), "id")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_level_course_id = Window.partitionBy("course_level", "course_id")
w_course_level = Window.partitionBy("course_level")
w_all_by_enrolled_at_moving_6 = Window.orderBy("enrolled_at", "id").rowsBetween(-6, Window.currentRow)
w_grade_status_enrolled_at_den = Window.partitionBy("grade_status", "enrolled_at_dense_rank")
w_second_enrolled_at_to_date_b = Window.partitionBy(F.to_date("second_enrolled_at")).orderBy("second_enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_cume_dist = Window.orderBy(F.col("enrolled_at_cume_dist_2").desc(), "id")
w_all_by_n_course_level_not_in = Window.orderBy(F.col("n_course_level_not_in_advanced").desc(), "course_level", "student_id")

base = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_details = (
    base.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select("s.*", F.col("c.level").alias("course_level"))
)

ranked = (
    with_details
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_enrolled_at), 3).alias("enrolled_at_percent_rank"),
        F.sum("score").over(w_all_by_enrolled_at_prior_1).alias("score_sum_1_prior"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at), 3).alias("enrolled_at_cume_dist"),
        F.dense_rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_dense_rank"),
        F.dense_rank().over(w_course_level_by_score).alias("score_dense_rank"),
        F.nth_value("score", 2).over(w_all_by_enrolled_at_full).alias("second_score"),
        F.round(F.avg("score").over(w_course_level_course_id), 2).alias("course_level_course_id_avg_score"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_course_level_by_score))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.size(F.collect_set("student_id").over(w_course_level)).alias("distinct_student_id_in_course_level"),
        F.nth_value("enrolled_at", 2).over(w_all_by_enrolled_at_full).alias("second_enrolled_at"),
        F.round(F.avg("score").over(w_all_by_enrolled_at_moving_6), 2).alias("score_avg_7"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist_2"),
        F.max("score").over(w_course_level).alias("course_level_max_score"),
        F.max("score").over(w_course_level_course_id).alias("course_level_course_id_max_score"),
    )
)

leaders = (
    ranked
    .filter(F.col("score_dense_rank") <= 3)
    .drop("score_dense_rank")
)

windowed = (
    leaders
    .select(
        "*",
        F.count(F.lit(1)).over(w_grade_status_enrolled_at_den).alias("rows_in_grade_status_enrolled_at_dense_rank"),
        F.sum("enrolled_at_cume_dist_2").over(w_second_enrolled_at_to_date_b).alias("running_enrolled_at_cume_dist_2"),
        F.max("score").over(w_grade_status_enrolled_at_den).alias("grade_status_enrolled_at_dense_rank_max_score"),
        F.lead("score_avg_7", 1, 0).over(w_all_by_enrolled_at_cume_dist).alias("next_score_avg_7"),
    )
)

summary = (
    windowed
    .groupBy("course_level", "student_id")
    .agg(
        F.sum(F.when(F.col("course_level").isNotNull(), 1).otherwise(0)).alias("n_course_level_is_not_null"),
        F.sum(F.when(~F.col("course_level").isin("advanced", "intro"), 1).otherwise(0)).alias("n_course_level_not_in_advanced"),
    )
)

with_windows = (
    summary
    .select(
        "*",
        F.count(F.lit(1)).over(w_course_level).alias("rows_in_course_level"),
        (F.col("n_course_level_is_not_null") - F.lag("n_course_level_is_not_null").over(w_all_by_n_course_level_not_in)).alias("n_course_level_is_not_null_change"),
        F.lag("n_course_level_is_not_null", 3).over(w_all_by_n_course_level_not_in).alias("prev3_n_course_level_is_not_null"),
    )
)

result = (
    with_windows
    .select(
        "course_level",
        "student_id",
        "n_course_level_is_not_null",
        "n_course_level_not_in_advanced",
        "rows_in_course_level",
        "n_course_level_is_not_null_change",
        "prev3_n_course_level_is_not_null",
        F.concat(F.col("course_level"), F.lit("#"), F.col("rows_in_course_level").cast("string")).alias("course_level_ref"),
    )
    .orderBy(
        F.col("course_level_ref").asc_nulls_last(),
        F.col("n_course_level_not_in_advanced").desc_nulls_first(),
    )
)
