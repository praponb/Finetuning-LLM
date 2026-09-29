from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy("score", "id")
w_course_id_by_score_distance = Window.partitionBy("course_id").orderBy("score_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_score_distance_2 = Window.partitionBy("course_id").orderBy("score_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_month = Window.partitionBy(F.month("enrolled_at"))
w_course_id = Window.partitionBy("course_id")
w_all_by_score_distance_full = Window.orderBy("score_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_distance = Window.orderBy("score_distance", "id")
w_all_by_score_distance_runnin = Window.orderBy("score_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_grade_status_by_e = Window.partitionBy("student_id", "grade_status").orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_grade_status_by_score_full = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_grade_status_in_cou = Window.partitionBy("distinct_grade_status_in_course_id").orderBy("distinct_grade_status_in_course_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_enrolled_at_2_month = Window.partitionBy(F.month("second_enrolled_at_2"))
w_distinct_grade_status_in_cou_2 = Window.partitionBy("distinct_grade_status_in_course_id").orderBy("distinct_grade_status_in_course_id", "id")
w_enrolled_at_to_date_course_i = Window.partitionBy(F.to_date("enrolled_at"), "course_id").orderBy(F.col("prev3_enrolled_at").desc(), "id")
w_distinct_grade_status_in_cou_3 = Window.partitionBy("distinct_grade_status_in_course_id", "grade_status").orderBy("second_enrolled_at_2", "id")
w_all_by_rows_in_enrolled_at_m = Window.orderBy("rows_in_enrolled_at_month", "id")

filtered_enrollments = (
    df_enrollments
    .filter((F.col("score") > 8.7) & (F.col("grade_status") != "WITHDRAWN"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.abs(F.col("score") - 36.6).alias("score_distance"),
    )
)

with_trends = (
    filtered_enrollments
    .select(
        "*",
        F.row_number().over(w_all_by_score).alias("rn"),
        F.nth_value("enrolled_at", 2).over(w_course_id_by_score_distance).alias("second_enrolled_at"),
        F.min("score").over(w_course_id_by_score_distance_2).alias("min_score_so_far"),
        F.count(F.lit(1)).over(w_enrolled_at_month).alias("rows_in_enrolled_at_month"),
        F.min("score").over(w_course_id).alias("course_id_min_score"),
        F.first("enrolled_at").over(w_all_by_score_distance_full).alias("first_enrolled_at"),
        F.round(F.cume_dist().over(w_all_by_score_distance), 3).alias("score_distance_cume_dist"),
        F.last("score_distance", ignorenulls=True).over(w_all_by_score_distance_runnin).alias("last_known_score_distance"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.min("score_distance").over(w_course_id).alias("course_id_min_score_distance"),
        F.round(F.avg("score").over(w_course_id), 2).alias("course_id_avg_score"),
        F.size(F.collect_set("grade_status").over(w_course_id)).alias("distinct_grade_status_in_course_id"),
        F.lag("enrolled_at", 3).over(w_student_id_grade_status_by_e).alias("prev3_enrolled_at"),
        F.nth_value("enrolled_at", 2).over(w_grade_status_by_score_full).alias("second_enrolled_at_2"),
    )
)

top_per_group = (
    with_trends
    .filter((F.col("rn") == 1) & (F.col("score_distance") > 45.0))
    .drop("rn")
)

with_trends_2 = (
    top_per_group
    .select(
        "*",
        F.last("last_known_score_distance").over(w_distinct_grade_status_in_cou).alias("last_last_known_score_distance"),
        F.count(F.lit(1)).over(w_second_enrolled_at_2_month).alias("rows_in_second_enrolled_at_2_month"),
        F.lag("score", 3).over(w_distinct_grade_status_in_cou_2).alias("prev3_score"),
        F.round(F.percent_rank().over(w_enrolled_at_to_date_course_i), 3).alias("prev3_enrolled_at_percent_rank"),
        F.lag("score_distance").over(w_distinct_grade_status_in_cou_3).alias("prev_score_distance"),
        F.row_number().over(w_all_by_rows_in_enrolled_at_m).alias("rn"),
    )
)

summary = (
    with_trends_2
    .groupBy(
        F.when(F.col("score_distance_cume_dist") >= 0.8, "HIGH").when(F.col("score_distance_cume_dist") >= 0.3, "MEDIUM").otherwise("LOW").alias("score_distance_cume_dist_tier"),
        F.year("first_enrolled_at").alias("first_enrolled_at_year"),
    )
    .agg(
        F.sum(F.when(F.col("grade_status") == "FAIL", 1).otherwise(0)).alias("n_grade_status_fail"),
        F.min("rows_in_second_enrolled_at_2_month").alias("min_rows_in_second_enrolled_at_2_month"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.round(F.stddev("course_id_min_score_distance"), 2).alias("stddev_course_id_min_score_distance"),
    )
)

result = (
    summary
    .select(
        "score_distance_cume_dist_tier",
        "n_grade_status_fail",
        "min_rows_in_second_enrolled_at_2_month",
        "stddev_course_id_min_score_distance",
        F.coalesce(F.col("first_enrolled_at_year"), F.lit(0)).alias("first_enrolled_at_year_filled"),
    )
    .orderBy(F.col("score_distance_cume_dist_tier").desc_nulls_first())
)
