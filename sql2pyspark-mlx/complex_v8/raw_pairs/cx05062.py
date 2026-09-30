from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_group_term = Window.partitionBy("grade_status_group", "term")
w_term_by_score_running = Window.partitionBy("term").orderBy(F.col("score").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_grade_status_group = Window.partitionBy("course_id", "grade_status_group").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_by_score_ties = Window.partitionBy("term").orderBy(F.col("score").desc_nulls_first())
w_course_id_grade_status_group_2 = Window.partitionBy("course_id", "grade_status_group").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_grade_status_group_by_enroll = Window.partitionBy("grade_status_group").orderBy(F.col("enrolled_at").desc(), "id")
w_course_id_grade_status_group_3 = Window.partitionBy("course_id", "grade_status_group").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-2, -1)
w_course_id_grade_status_group_4 = Window.partitionBy("course_id", "grade_status_group").orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_running_count_full = Window.orderBy(F.col("running_count").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_quintile_student = Window.partitionBy("enrolled_at_quintile", "student_id").orderBy("last_score", "id")
w_term_student_id = Window.partitionBy("term", "student_id")
w_all_by_enrolled_at_quintile = Window.orderBy("enrolled_at_quintile", "id")
w_all_by_running_count = Window.orderBy(F.col("running_count").desc(), "id")

recent = (
    df_enrollments
    .filter((F.col("score") <= 63.9) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.when(F.col("grade_status").isin("PASS", "FAIL"), "group_a").when(F.col("grade_status") == "WITHDRAWN", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

ranked = (
    recent
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "grade_status_group",
        F.size(F.collect_set("course_id").over(w_grade_status_group_term)).alias("distinct_course_id_in_grade_status_group_term"),
        F.count(F.lit(1)).over(w_term_by_score_running).alias("running_count"),
        F.last("score").over(w_course_id_grade_status_group).alias("last_score"),
        F.dense_rank().over(w_term_by_score_ties).alias("score_dense_rank"),
        F.round(F.avg("score").over(w_course_id_grade_status_group_2), 2).alias("score_avg_7"),
        F.ntile(5).over(w_grade_status_group_by_enroll).alias("enrolled_at_quintile"),
        F.max("score").over(w_course_id_grade_status_group_3).alias("score_max_2_prior"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_group_term), F.lit(0)), 4).alias("score_share_of_grade_status_group_term"),
        F.lag("score").over(w_course_id_grade_status_group_4).alias("prev_score"),
        F.sum("score").over(w_grade_status_group_term).alias("grade_status_group_term_sum_score"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status_group",
        "distinct_course_id_in_grade_status_group_term",
        "running_count",
        "last_score",
        "score_avg_7",
        "score_max_2_prior",
        "prev_score",
        "grade_status_group_term_sum_score",
        F.nth_value("enrolled_at_quintile", 2).over(w_all_by_running_count_full).alias("second_enrolled_at_quintile"),
        F.round(F.percent_rank().over(w_enrolled_at_quintile_student), 3).alias("last_score_percent_rank"),
        F.sum("score_avg_7").over(w_term_student_id).alias("term_student_id_sum_score_avg_7"),
        F.lag("running_count").over(w_all_by_enrolled_at_quintile).alias("prev_running_count"),
        (F.col("score_dense_rank") - F.lag("score_dense_rank").over(w_all_by_running_count)).alias("score_dense_rank_change"),
    )
)

grouped = (
    sequenced
    .groupBy("student_id", "grade_status_group", "second_enrolled_at_quintile")
    .agg(
        F.round(F.stddev("last_score_percent_rank"), 2).alias("stddev_last_score_percent_rank"),
        F.round(F.sum("grade_status_group_term_sum_score"), 2).alias("total_grade_status_group_term_sum_score"),
        F.sum("second_enrolled_at_quintile").alias("total_second_enrolled_at_quintile"),
        F.percentile_approx("second_enrolled_at_quintile", 0.5).alias("median_second_enrolled_at_quintile"),
        F.round(F.stddev("last_score"), 2).alias("stddev_last_score"),
    )
)

result = (
    grouped
    .select(
        "student_id",
        "grade_status_group",
        "second_enrolled_at_quintile",
        "total_second_enrolled_at_quintile",
        "stddev_last_score",
        F.initcap(F.trim(F.col("grade_status_group"))).alias("grade_status_group_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status_group")), " ", "_")).alias("grade_status_group_slug"),
    )
    .orderBy("grade_status_group_slug")
)
