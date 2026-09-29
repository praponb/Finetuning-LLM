from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_student_id_by_score_ful = Window.partitionBy("term", "student_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_grade_status = Window.partitionBy("term", "grade_status")
w_all_by_score = Window.orderBy("score", "id")
w_course_id_by_score = Window.partitionBy("course_id").orderBy(F.col("score").desc(), "id")
w_course_id_grade_status_by_sc = Window.partitionBy("course_id", "grade_status").orderBy("score_share_of_term_grade_status")
w_grade_status_distinct_studen = Window.partitionBy("grade_status", "distinct_student_id_in_term_grade_status").orderBy("term_grade_status_max_score", "id")
w_course_id_grade_status_by_sc_2 = Window.partitionBy("course_id", "grade_status").orderBy("score_share_of_term_grade_status", "id")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2024T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

with_windows = (
    filtered_enrollments
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.last("score").over(w_term_student_id_by_score_ful).alias("last_score"),
        F.max("score").over(w_term_grade_status).alias("term_grade_status_max_score"),
        F.count(F.lit(1)).over(w_term_grade_status).alias("rows_in_term_grade_status"),
        F.ntile(10).over(w_all_by_score).alias("score_decile"),
        F.size(F.collect_set("student_id").over(w_term_grade_status)).alias("distinct_student_id_in_term_grade_status"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term_grade_status), F.lit(0)), 4).alias("score_share_of_term_grade_status"),
        F.lag("score", 1, 0).over(w_course_id_by_score).alias("prev_score"),
        F.min("score").over(w_term_grade_status).alias("term_grade_status_min_score"),
    )
)

tagged = (
    with_windows
    .filter(F.col("course_id").isNotNull())
    .withColumn("prev_score_bucket", F.floor(F.col("prev_score") / 25) * 25)
)

with_trends = (
    tagged
    .select(
        "id",
        "term",
        "grade_status",
        "term_grade_status_max_score",
        "rows_in_term_grade_status",
        "score_decile",
        "score_share_of_term_grade_status",
        "prev_score",
        "term_grade_status_min_score",
        F.rank().over(w_course_id_grade_status_by_sc).alias("score_share_of_term_grade_status_rank"),
        F.lead("score_decile", 1, 0).over(w_grade_status_distinct_studen).alias("next_score_decile"),
        F.dense_rank().over(w_course_id_grade_status_by_sc_2).alias("score_share_of_term_grade_status_dense_rank"),
    )
)

rollup_base = (
    with_trends
    .groupBy("grade_status")
    .agg(
        F.percentile_approx("score_share_of_term_grade_status_rank", 0.5).alias("median_score_share_of_term_grade_status_rank"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.sum("term_grade_status_min_score").alias("sum_term_grade_status_min_score_check"),
    )
    .filter(F.col("sum_term_grade_status_min_score_check") > 200.0)
    .drop("sum_term_grade_status_min_score_check")
)

result = (
    rollup_base
    .filter(F.size(F.col("term_list")) >= 2)
    .select(
        "grade_status",
        "median_score_share_of_term_grade_status_rank",
        "distinct_ids",
        "term_list",
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_first(), "grade_status")
    .limit(5)
)
