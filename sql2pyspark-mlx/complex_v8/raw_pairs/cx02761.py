from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_enrollment_enrolled_at = Window.partitionBy(F.date_trunc("month", "first_enrollment_enrolled_at")).orderBy(F.col("score_distance").asc_nulls_last())
w_first_enrollment_enrolled_at_2 = Window.partitionBy(F.date_trunc("month", "first_enrollment_enrolled_at")).orderBy(F.col("score_distance").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
        F.abs(F.col("score") - 44.4).alias("score_distance"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.sum("score").alias("total_enrollment_score"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    prepared_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        "e.total_enrollment_score",
        "e.first_enrollment_enrolled_at",
    )
)

windowed = (
    summarized_rows
    .select(
        "*",
        F.dense_rank().over(w_first_enrollment_enrolled_at).alias("score_distance_dense_rank"),
        F.last("total_enrollment_score").over(w_first_enrollment_enrolled_at_2).alias("last_total_enrollment_score"),
    )
)

line_level = (
    windowed.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_distance",
        "s.last_enrollment_enrolled_at",
        "s.first_enrollment_enrolled_at",
        "s.last_total_enrollment_score",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    line_level
    .groupingSets(
        [["enrollment_term", "student_id", "enrollment_course_id"], ["enrollment_term"], []],
        "enrollment_term", "student_id", "enrollment_course_id",
    )
    .agg(
        F.sum(F.when(~F.col("enrollment_term").isin("2025T2", "2024T1"), 1).otherwise(0)).alias("n_enrollment_term_not_in_2025t"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
    )
    .orderBy(
        F.col("enrollment_term").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
        F.col("enrollment_course_id").asc_nulls_last(),
    )
)
