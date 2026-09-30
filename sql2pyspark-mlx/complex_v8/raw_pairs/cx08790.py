from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_student_id_by_enrolled = Window.partitionBy("term", "student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_enrolled_at_year = Window.partitionBy("course_id", F.year("enrolled_at"))
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")
w_second_enrolled_at_year = Window.partitionBy(F.year("second_enrolled_at"))
w_all_by_second_enrolled_at_ru = Window.orderBy(F.col("second_enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_second_enrolled_at_fu = Window.orderBy(F.col("second_enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_second_enrolled_at = Window.orderBy(F.col("second_enrolled_at").desc(), "id")
w_enrolled_at_month = Window.partitionBy(F.date_trunc("month", "enrolled_at"))
w_term_by_distinct_term_in_cou = Window.partitionBy("term").orderBy("distinct_term_in_course_id_enrolled_at_year")
w_term_by_distinct_term_in_cou_2 = Window.partitionBy("term").orderBy("distinct_term_in_course_id_enrolled_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_enrollments
    .filter(F.col("student_id") <= 17)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.nth_value("enrolled_at", 2).over(w_term_student_id_by_enrolled).alias("second_enrolled_at"),
        F.size(F.collect_set("term").over(w_course_id_enrolled_at_year)).alias("distinct_term_in_course_id_enrolled_at_year"),
        F.round(F.cume_dist().over(w_grade_status_by_score), 3).alias("score_cume_dist"),
        F.min("score").over(w_course_id_enrolled_at_year).alias("course_id_enrolled_at_year_min_score"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.min("score_cume_dist").over(w_second_enrolled_at_year).alias("second_enrolled_at_year_min_score_cume_dist"),
        F.last("score", ignorenulls=True).over(w_all_by_second_enrolled_at_ru).alias("last_known_score"),
        F.nth_value("second_enrolled_at", 2).over(w_all_by_second_enrolled_at_fu).alias("second_second_enrolled_at"),
        F.ntile(4).over(w_all_by_second_enrolled_at).alias("second_enrolled_at_quartile"),
        F.max("score").over(w_all_by_second_enrolled_at_ru).alias("max_score_so_far"),
        F.count(F.lit(1)).over(w_second_enrolled_at_year).alias("rows_in_second_enrolled_at_year"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "id",
        "term",
        "score",
        "enrolled_at",
        "term_ref",
        "second_enrolled_at",
        "score_cume_dist",
        "course_id_enrolled_at_year_min_score",
        "last_known_score",
        "second_second_enrolled_at",
        "max_score_so_far",
        F.sum("max_score_so_far").over(w_enrolled_at_month).alias("enrolled_at_month_sum_max_score_so_far"),
        F.round(F.percent_rank().over(w_term_by_distinct_term_in_cou), 3).alias("distinct_term_in_course_id_enrolled_at_year_perc"),
        F.round(F.col("score_cume_dist") / F.nullif(F.sum("score_cume_dist").over(w_enrolled_at_month), F.lit(0)), 4).alias("score_cume_dist_share_of_enrolled_at_month"),
        F.nth_value("last_known_score", 2).over(w_term_by_distinct_term_in_cou_2).alias("second_last_known_score"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "term",
        "score",
        "enrolled_at",
        "second_enrolled_at",
        "score_cume_dist",
        "last_known_score",
        "second_second_enrolled_at",
        "second_last_known_score",
        F.substring(F.trim(F.col("term_ref")), 1, 2).alias("term_ref_prefix"),
    )
)
