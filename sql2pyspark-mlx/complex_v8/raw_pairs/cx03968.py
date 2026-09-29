from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_student_id_course_id = Window.partitionBy("student_id", "course_id")
w_course_id = Window.partitionBy("course_id")
w_enrolled_at_year_by_enrolled = Window.partitionBy(F.year("enrolled_at")).orderBy(F.col("enrolled_at").desc(), "id")
w_student_id_by_score_prior_2 = Window.partitionBy("student_id").orderBy("score", "id").rowsBetween(-2, -1)
w_enrolled_at_year_by_enrolled_2 = Window.partitionBy(F.year("enrolled_at")).orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_score = Window.partitionBy("student_id").orderBy("score", "id")

valid = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.last("enrolled_at", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("last_known_enrolled_at"),
        (F.col("score") - F.lag("score").over(w_all_by_enrolled_at)).alias("score_change"),
        F.size(F.collect_set("grade_status").over(w_student_id_course_id)).alias("distinct_grade_status_in_student_id_course_id"),
        F.round(F.col("score") - F.avg("score").over(w_course_id), 2).alias("score_vs_course_id_avg"),
        F.ntile(10).over(w_enrolled_at_year_by_enrolled).alias("enrolled_at_decile"),
        F.round(F.avg("score").over(w_student_id_by_score_prior_2), 2).alias("score_avg_2_prior"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.sum("score").over(w_enrolled_at_year_by_enrolled_2).alias("running_score"),
        F.min("score").over(w_student_id_course_id).alias("student_id_course_id_min_score"),
        F.lead("enrolled_at").over(w_enrolled_at_year_by_enrolled).alias("next_enrolled_at"),
        F.round(F.percent_rank().over(w_student_id_by_score), 3).alias("score_percent_rank"),
        F.sum("score").over(w_course_id).alias("course_id_sum_score"),
    )
)

tagged = (
    with_windows
    .filter(F.col("minutes_since_prev_enrolled_at").between(1000.0, 4000.0))
    .select(
        "*",
        F.when(F.col("score_avg_2_prior") >= 55.0, "A").when(F.col("score_avg_2_prior") >= 20.0, "B").otherwise("C").alias("score_avg_2_prior_tier"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

result = (
    tagged
    .filter(F.col("distinct_grade_status_in_student_id_course_id") <= 9)
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "last_known_enrolled_at",
        "score_change",
        "distinct_grade_status_in_student_id_course_id",
        "enrolled_at_decile",
        "score_avg_2_prior",
        "minutes_since_prev_enrolled_at",
        "running_score",
        "student_id_course_id_min_score",
        "course_id_sum_score",
        "score_avg_2_prior_tier",
        "days_since_enrolled_at",
        F.abs(F.col("score_change") - 10.0).alias("score_change_distance"),
        F.greatest(F.col("enrolled_at_decile"), F.col("score_avg_2_prior")).alias("greatest_enrolled_at_decile_score_avg_2_prior"),
    )
    .orderBy(F.col("score_avg_2_prior").asc())
)
