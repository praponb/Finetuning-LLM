from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_group_term_len_by_enrol = Window.partitionBy("term_group", "term_len").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_term_student_id = Window.partitionBy("term", "student_id")
w_term_group_term_len_by_enrol_2 = Window.partitionBy("term_group", "term_len").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_total_distinct_course = Window.orderBy(F.col("total_distinct_course_id_in_term_student_id").desc(), "enrolled_at_month")
w_all_by_total_distinct_course_2 = Window.orderBy("total_distinct_course_id_in_term_student_id")
w_total_distinct_course_id_in = Window.partitionBy("total_distinct_course_id_in_term_student_id_rank", "n_grade_status_is_not_null")
w_all_by_score_to_distinct_cou = Window.orderBy("score_to_distinct_course_id_in_term_student_id_r")
w_n_grade_status_is_not_null_b = Window.partitionBy("n_grade_status_is_not_null").orderBy("n_grade_status_is_not_null")

scoped_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("term").isin("2025T1", "2024T1"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

with_trends = (
    scoped_enrollments
    .select(
        "*",
        F.round(F.avg("score").over(w_term_group_term_len_by_enrol), 2).alias("score_avg_2_prior"),
        F.count(F.lit(1)).over(w_term_student_id).alias("rows_in_term_student_id"),
        F.size(F.collect_set("course_id").over(w_term_student_id)).alias("distinct_course_id_in_term_student_id"),
        F.last("term_len").over(w_term_group_term_len_by_enrol_2).alias("last_term_len"),
    )
)

per_group = (
    with_trends
    .groupBy(F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"))
    .agg(
        F.count(F.when(F.col("grade_status").isNotNull(), 1)).alias("n_grade_status_is_not_null"),
        F.round(F.sum("score") / F.nullif(F.sum("distinct_course_id_in_term_student_id"), F.lit(0)), 3).alias("score_to_distinct_course_id_in_term_student_id_r"),
        F.sum("distinct_course_id_in_term_student_id").alias("total_distinct_course_id_in_term_student_id"),
        F.avg("score").alias("avg_score_check"),
    )
    .filter(F.col("avg_score_check") > 8.7)
    .drop("avg_score_check")
)

metrics = (
    per_group
    .withColumn("total_distinct_course_id_in_term_student_id_deci", F.ntile(10).over(w_all_by_total_distinct_course))
    .withColumn("total_distinct_course_id_in_term_student_id_rank", F.rank().over(w_all_by_total_distinct_course_2))
)

with_trends_2 = (
    metrics
    .select(
        "enrolled_at_month",
        "n_grade_status_is_not_null",
        "score_to_distinct_course_id_in_term_student_id_r",
        "total_distinct_course_id_in_term_student_id",
        "total_distinct_course_id_in_term_student_id_deci",
        F.round(F.col("n_grade_status_is_not_null") / F.nullif(F.sum("n_grade_status_is_not_null").over(w_total_distinct_course_id_in), F.lit(0)), 4).alias("n_grade_status_is_not_null_share_of_total_distin"),
        F.round(F.cume_dist().over(w_all_by_score_to_distinct_cou), 3).alias("score_to_distinct_course_id_in_term_student_id_r_2"),
        F.round(F.cume_dist().over(w_n_grade_status_is_not_null_b), 3).alias("n_grade_status_is_not_null_cume_dist"),
        F.count(F.lit(1)).over(w_total_distinct_course_id_in).alias("rows_in_total_distinct_course_id_in_term_student"),
    )
)

result = (
    with_trends_2
    .rollup("n_grade_status_is_not_null", "total_distinct_course_id_in_term_student_id_deci")
    .agg(
        F.grouping("n_grade_status_is_not_null").alias("is_total_n_grade_status_is_not_null"),
        F.min("enrolled_at_month").alias("first_enrolled_at_month"),
        F.round(F.avg("score_to_distinct_course_id_in_term_student_id_r"), 2).alias("avg_score_to_distinct_course_id_in_term_student_"),
    )
    .orderBy(
        F.col("n_grade_status_is_not_null").asc_nulls_last(),
        F.col("total_distinct_course_id_in_term_student_id_deci").asc_nulls_last(),
    )
)
