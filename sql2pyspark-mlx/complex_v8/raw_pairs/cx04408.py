from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_by_enrolled_at_tie = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc())
w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc(), "id")
w_enrolled_at_rank_3_first_enr = Window.partitionBy("enrolled_at_rank_3", F.date_trunc("month", "first_enrolled_at"))
w_enrolled_at_rank_2_by_enroll = Window.partitionBy("enrolled_at_rank_2").orderBy(F.col("enrolled_at_percent_rank").desc(), "id").rowsBetween(-3, -1)
w_enrolled_at_rank_2_by_enroll_2 = Window.partitionBy("enrolled_at_rank_2").orderBy(F.col("enrolled_at_percent_rank").desc())
w_enrolled_at_rank_2_by_enroll_3 = Window.partitionBy("enrolled_at_rank_2").orderBy(F.col("enrolled_at_percent_rank").desc(), "id")
w_enrolled_at_rank_by_score_bu = Window.partitionBy("enrolled_at_rank").orderBy(F.col("score_bucket_length_trim_course_code_4").desc(), "enrolled_at_rank").rowsBetween(-1, Window.currentRow)
w_enrolled_at_rank_by_score_bu_2 = Window.partitionBy("enrolled_at_rank").orderBy(F.col("score_bucket_length_trim_course_code_4").desc(), "enrolled_at_rank")

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
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

enriched_course = (
    scoped_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.course_code").isNotNull())
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_bucket",
        "c.course_code",
    )
)

with_trends = (
    enriched_course
    .select(
        "*",
        F.round(F.col("score_bucket") - F.avg("score_bucket").over(w_grade_status), 2).alias("score_bucket_vs_grade_status_avg"),
        F.round(F.col("score_bucket") / F.nullif(F.sum("score_bucket").over(w_grade_status), F.lit(0)), 4).alias("score_bucket_share_of_grade_status"),
        F.rank().over(w_all_by_enrolled_at).alias("enrolled_at_rank"),
        F.nth_value("score_bucket", 2).over(w_all_by_enrolled_at_full).alias("second_score_bucket"),
        F.rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_rank_2"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_percent_rank"),
        F.last("score_bucket", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("last_known_score_bucket"),
        F.first("enrolled_at").over(w_all_by_enrolled_at_full).alias("first_enrolled_at"),
        F.lag("enrolled_at").over(w_all_by_enrolled_at).alias("prev_enrolled_at"),
        F.rank().over(w_course_id_by_enrolled_at_tie).alias("enrolled_at_rank_3"),
        F.row_number().over(w_course_id_by_enrolled_at).alias("seq_no"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "score_bucket",
        "course_code",
        "score_bucket_share_of_grade_status",
        "enrolled_at_rank",
        "second_score_bucket",
        "last_known_score_bucket",
        "prev_enrolled_at",
        "seq_no",
        F.size(F.collect_set("grade_status").over(w_enrolled_at_rank_3_first_enr)).alias("distinct_grade_status_in_enrolled_at_rank_3_firs"),
        F.sum("enrolled_at_rank_3").over(w_enrolled_at_rank_2_by_enroll).alias("enrolled_at_rank_3_sum_3_prior"),
        F.round(F.percent_rank().over(w_enrolled_at_rank_2_by_enroll_2), 3).alias("enrolled_at_percent_rank_percent_rank"),
        F.round(F.percent_rank().over(w_enrolled_at_rank_2_by_enroll_3), 3).alias("enrolled_at_percent_rank_percent_rank_2"),
        F.max("enrolled_at_percent_rank").over(w_enrolled_at_rank_3_first_enr).alias("enrolled_at_rank_3_first_enrolled_at_month_max_e"),
    )
)

summary = (
    with_trends_2
    .groupBy("enrolled_at_rank")
    .agg(
        F.max("score_bucket").alias("max_score_bucket"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.length(F.trim(F.col("course_code"))) >= 4, F.col("score_bucket")).otherwise(0)).alias("score_bucket_length_trim_course_code_4"),
    )
)

ranked = (
    summary
    .select(
        "*",
        F.sum("max_score_bucket").over(w_enrolled_at_rank_by_score_bu).alias("max_score_bucket_sum_2"),
        (F.col("cnt") - F.lag("cnt").over(w_enrolled_at_rank_by_score_bu_2)).alias("cnt_change"),
        F.rank().over(w_enrolled_at_rank_by_score_bu_2).alias("score_bucket_length_trim_course_code_4_rank"),
    )
)

result = (
    ranked
    .filter(F.col("score_bucket_length_trim_course_code_4_rank") <= 8)
    .select(
        "enrolled_at_rank",
        "score_bucket_length_trim_course_code_4",
        "max_score_bucket_sum_2",
        "cnt_change",
        F.abs(F.col("cnt") - 6).alias("cnt_distance"),
        F.coalesce(F.col("max_score_bucket"), F.lit(0)).alias("max_score_bucket_filled"),
    )
    .orderBy(
        "max_score_bucket_sum_2",
        F.col("score_bucket_length_trim_course_code_4").desc(),
    )
)
