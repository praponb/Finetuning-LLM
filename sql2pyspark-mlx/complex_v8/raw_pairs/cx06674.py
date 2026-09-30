from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_student_id = Window.partitionBy("course_id", "student_id")
w_term_course_id_by_score_dist = Window.partitionBy("term", "course_id").orderBy("score_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_course_id_by_score_dist_2 = Window.partitionBy("term", "course_id").orderBy("score_distance")
w_grade_status_course_id_by_sc = Window.partitionBy("grade_status", "course_id").orderBy(F.col("score_distance").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_credits_by_enrolled_a = Window.partitionBy("course_credits").orderBy("enrolled_at", "id").rowsBetween(-2, Window.currentRow)
w_course_credits_by_enrolled_a_2 = Window.partitionBy("course_credits").orderBy("enrolled_at", "id")
w_enrolled_at_month = Window.partitionBy(F.month("enrolled_at"))
w_course_credits_by_enrolled_a_3 = Window.partitionBy("course_credits").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_enrollments
    .filter((F.col("course_id") >= 7) & (~F.col("grade_status").isin("FAIL", "WITHDRAWN")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 36.6).alias("score_distance"),
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.round(F.col("score_distance") - F.avg("score_distance").over(w_course_id_student_id), 2).alias("score_distance_vs_course_id_student_id_avg"),
        F.nth_value("enrolled_at", 2).over(w_term_course_id_by_score_dist).alias("second_enrolled_at"),
        F.dense_rank().over(w_term_course_id_by_score_dist_2).alias("score_distance_dense_rank"),
        F.first("enrolled_at").over(w_grade_status_course_id_by_sc).alias("first_enrolled_at"),
        F.round(F.avg("score_distance").over(w_course_id_student_id), 2).alias("course_id_student_id_avg_score_distance"),
    )
)

combined_details_course = (
    sequenced.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.upper(F.trim(F.col("c.course_code"))).like("B%"))
    .select("s.*", F.col("c.level").alias("course_level"), "c.course_code")
)

joined_course = (
    combined_details_course.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.course_code").isNotNull())
    .select(
        "s.*",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.course_code").alias("course_course_code"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
    )
)

with_stats = (
    joined_course.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

with_windows = (
    with_stats
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "score_distance_vs_course_id_student_id_avg",
        "second_enrolled_at",
        "course_level",
        "course_code",
        "course_dept",
        "course_course_code",
        "enrollment_count",
        "enrollment_count_2",
        F.round(F.avg("score_distance_dense_rank").over(w_course_credits_by_enrolled_a), 2).alias("score_distance_dense_rank_avg_3"),
        F.row_number().over(w_course_credits_by_enrolled_a_2).alias("enrolled_at_seq"),
        F.round(F.col("score_distance_dense_rank") - F.avg("score_distance_dense_rank").over(w_enrolled_at_month), 2).alias("score_distance_dense_rank_vs_enrolled_at_month_a"),
        F.dense_rank().over(w_course_credits_by_enrolled_a_2).alias("enrolled_at_dense_rank"),
        F.nth_value("enrolled_at", 2).over(w_course_credits_by_enrolled_a_3).alias("second_enrolled_at_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "score_distance_dense_rank_vs_enrolled_at_month_a",
        "score_distance_vs_course_id_student_id_avg",
    )
    .unpivot(
        ids=["id"],
        values=["score_distance_dense_rank_vs_enrolled_at_month_a", "score_distance_vs_course_id_student_id_avg"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .orderBy("id", "metric")
)
