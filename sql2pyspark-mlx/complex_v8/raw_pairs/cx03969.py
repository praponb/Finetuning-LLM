from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_score_ties = Window.partitionBy("student_id").orderBy(F.col("score").desc())
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_enrolled_at_year_course_id_b = Window.partitionBy(F.year("enrolled_at"), "course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_year_course_id_b_2 = Window.partitionBy(F.year("enrolled_at"), "course_id").orderBy("score", "id")
w_grade_status = Window.partitionBy("grade_status")
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_all_by_enrolled_at_moving_6 = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_term_by_enrolled_at = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_student_id_by_score_running = Window.partitionBy("student_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id = Window.partitionBy("course_id")
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_enrolled_at_year_by_avg = Window.partitionBy("term", "enrolled_at_year").orderBy(F.col("avg_minutes_since_prev_enrolled_at").desc())
w_term = Window.partitionBy("term")

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isNotNull()) & (F.col("course_id") >= 9))
)

metrics = (
    cleaned_enrollments
    .select(
        "id",
        "term",
        "grade_status",
        "enrolled_at",
        F.round(F.percent_rank().over(w_student_id_by_score_ties), 3).alias("score_percent_rank"),
        F.lead("enrolled_at").over(w_all_by_enrolled_at).alias("next_enrolled_at"),
        F.last("score").over(w_enrolled_at_year_course_id_b).alias("last_score"),
        F.dense_rank().over(w_enrolled_at_year_course_id_b_2).alias("score_dense_rank"),
        F.size(F.collect_set("term").over(w_grade_status)).alias("distinct_term_in_grade_status"),
        F.count(F.lit(1)).over(w_grade_status_course_id).alias("rows_in_grade_status_course_id"),
        F.first("score").over(w_enrolled_at_year_course_id_b).alias("first_score"),
        F.max("score").over(w_all_by_enrolled_at_moving_6).alias("score_max_7"),
        F.lead("enrolled_at").over(w_term_by_enrolled_at).alias("next_enrolled_at_2"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_enrolled_at_year_course_id_b_2))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.row_number().over(w_all_by_enrolled_at).alias("seq_no"),
        F.min("score").over(w_student_id_by_score_running).alias("min_score_so_far"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.min("score").over(w_all_by_enrolled_at_running).alias("min_score_so_far_2"),
        (F.col("score") - F.lag("score").over(w_term_by_enrolled_at)).alias("score_change"),
    )
)

per_group = (
    metrics
    .groupBy("term", F.year("enrolled_at").alias("enrolled_at_year"))
    .agg(
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.sum(F.when(F.col("term").isin("2025T2", "2025T1"), F.col("distinct_term_in_grade_status")).otherwise(0)).alias("distinct_term_in_grade_status_term_in_2025t2_202"),
        F.round(F.avg("minutes_since_prev_enrolled_at"), 2).alias("avg_minutes_since_prev_enrolled_at"),
        F.max("score_max_7").alias("max_score_max_7_check"),
    )
    .filter(F.col("max_score_max_7_check") > 75.0)
    .drop("max_score_max_7_check")
)

with_windows = (
    per_group
    .withColumn("avg_minutes_since_prev_enrolled_at_dense_rank", F.dense_rank().over(w_term_enrolled_at_year_by_avg))
    .withColumn("distinct_grade_status_vs_term_avg", F.round(F.col("distinct_grade_status") - F.avg("distinct_grade_status").over(w_term), 2))
)

result = (
    with_windows
    .select(
        "term",
        "enrolled_at_year",
        "distinct_grade_status",
        "distinct_term_in_grade_status_term_in_2025t2_202",
        "avg_minutes_since_prev_enrolled_at",
        "avg_minutes_since_prev_enrolled_at_dense_rank",
        "distinct_grade_status_vs_term_avg",
    )
    .orderBy(
        F.col("avg_minutes_since_prev_enrolled_at_dense_rank").desc(),
        "avg_minutes_since_prev_enrolled_at",
    )
)
