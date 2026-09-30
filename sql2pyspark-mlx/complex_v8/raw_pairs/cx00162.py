from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_or_unknown_term = Window.partitionBy("grade_status_or_unknown", "term").orderBy(F.col("score").asc_nulls_last(), "id")
w_student_id = Window.partitionBy("student_id")
w_grade_status_or_unknown_term_2 = Window.partitionBy("grade_status_or_unknown", "term").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_or_unknown = Window.partitionBy("grade_status_or_unknown")
w_course_id_grade_status_or_un = Window.partitionBy("course_id", "grade_status_or_unknown").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_enrolled_at_plus_15m = Window.partitionBy(F.date_trunc("month", "second_enrolled_at_plus_15m_2"))
w_student_id_by_score = Window.partitionBy("student_id").orderBy("score", "id")
w_all_by_cnt_full = Window.orderBy("cnt", "distinct_student_id_in_grade_status_or_unknown", "enrolled_at_plus_15m_day", "student_id_avg_score_tier").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_cnt = Window.orderBy("cnt", "distinct_student_id_in_grade_status_or_unknown", "enrolled_at_plus_15m_day", "student_id_avg_score_tier")
w_all_by_cnt_running = Window.orderBy("cnt", "distinct_student_id_in_grade_status_or_unknown", "enrolled_at_plus_15m_day", "student_id_avg_score_tier").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_cnt_ties = Window.orderBy("cnt")

recent_enrollments = (
    df_enrollments
    .filter((F.month(F.col("enrolled_at")).isin(1, 3)) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 15 MINUTES")).alias("enrolled_at_plus_15m"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

windowed = (
    recent_enrollments
    .select(
        "*",
        F.row_number().over(w_grade_status_or_unknown_term).alias("score_seq"),
        F.size(F.collect_set("grade_status_or_unknown").over(w_student_id)).alias("distinct_grade_status_or_unknown_in_student_id"),
        F.nth_value("enrolled_at_plus_15m", 2).over(w_grade_status_or_unknown_term_2).alias("second_enrolled_at_plus_15m"),
        F.round(F.col("score") - F.avg("score").over(w_student_id), 2).alias("score_vs_student_id_avg"),
        F.size(F.collect_set("student_id").over(w_grade_status_or_unknown)).alias("distinct_student_id_in_grade_status_or_unknown"),
        F.round(F.avg("score").over(w_student_id), 2).alias("student_id_avg_score"),
        F.min("score").over(w_student_id).alias("student_id_min_score"),
        F.nth_value("enrolled_at_plus_15m", 2).over(w_course_id_grade_status_or_un).alias("second_enrolled_at_plus_15m_2"),
    )
)

first_rows = (
    windowed
    .filter(F.col("score_seq") <= 2)
    .drop("score_seq")
)

with_trends = (
    first_rows
    .select(
        "*",
        F.max("score_vs_student_id_avg").over(w_second_enrolled_at_plus_15m).alias("second_enrolled_at_plus_15m_2_month_max_score_vs"),
        (F.col("score") - F.lag("score").over(w_student_id_by_score)).alias("score_change"),
        F.round(F.col("student_id_min_score") - F.avg("student_id_min_score").over(w_student_id), 2).alias("student_id_min_score_vs_student_id_avg"),
        F.max("student_id_min_score").over(w_student_id).alias("student_id_max_student_id_min_score"),
        F.round(F.col("student_id_avg_score") - F.avg("student_id_avg_score").over(w_student_id), 2).alias("student_id_avg_score_vs_student_id_avg"),
        F.round(F.avg("score_vs_student_id_avg").over(w_student_id), 2).alias("student_id_avg_score_vs_student_id_avg_2"),
        F.lag("score_vs_student_id_avg", 3, 0).over(w_student_id_by_score).alias("prev3_score_vs_student_id_avg"),
        F.max("score_vs_student_id_avg").over(w_student_id).alias("student_id_max_score_vs_student_id_avg"),
    )
)

rollup_base = (
    with_trends
    .groupBy(
        "distinct_student_id_in_grade_status_or_unknown",
        F.to_date("enrolled_at_plus_15m").alias("enrolled_at_plus_15m_day"),
        F.when(F.col("student_id_avg_score") >= 85.0, "A").when(F.col("student_id_avg_score") >= 20.0, "B").otherwise("C").alias("student_id_avg_score_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.max("student_id_avg_score_vs_student_id_avg").alias("max_student_id_avg_score_vs_student_id_avg"),
        F.count(F.when(F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN", "FAIL"), 1)).alias("n_grade_status_in_incomplete_w"),
    )
)

sequenced = (
    rollup_base
    .select(
        "*",
        F.nth_value("distinct_student_ids", 2).over(w_all_by_cnt_full).alias("second_distinct_student_ids"),
        F.ntile(5).over(w_all_by_cnt).alias("cnt_quintile"),
        F.count(F.lit(1)).over(w_all_by_cnt_running).alias("running_count"),
        F.rank().over(w_all_by_cnt_ties).alias("cnt_rank"),
    )
)

result = (
    sequenced
    .select(
        "distinct_student_id_in_grade_status_or_unknown",
        "distinct_student_ids",
        "max_student_id_avg_score_vs_student_id_avg",
        "second_distinct_student_ids",
        "cnt_quintile",
        "running_count",
        "cnt_rank",
        F.least(F.col("running_count"), F.col("cnt_quintile")).alias("least_running_count_cnt_quintile"),
    )
    .orderBy(F.col("distinct_student_ids").desc())
)
