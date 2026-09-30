from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_student_id_by_enro = Window.partitionBy("course_id", "student_id").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_student_id_by_enro_2 = Window.partitionBy("course_id", "student_id").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_course_id_student_id = Window.partitionBy("course_id", "student_id")
w_course_id_by_score_running = Window.partitionBy("course_id").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_month_by_enrolle = Window.partitionBy(F.month("enrolled_at")).orderBy("enrolled_at")
w_all_by_course_credits = Window.orderBy("course_credits", "id")
w_enrolled_at_month_by_enrolle_2 = Window.partitionBy(F.month("enrolled_at")).orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_score = Window.partitionBy("course_id").orderBy(F.col("score").asc_nulls_last(), "id")
w_course_level_student_id = Window.partitionBy("course_level", "student_id")
w_course_level_course_id_by_co = Window.partitionBy("course_level", "course_id").orderBy(F.col("course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_enrollments = (
    df_enrollments
    .filter(~F.col("term").isin("2024T2", "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
    )
)

enriched = (
    valid_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_ref",
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.nth_value("course_credits", 2).over(w_course_id_student_id_by_enro).alias("second_course_credits"),
        F.lead("enrolled_at").over(w_course_id_student_id_by_enro_2).alias("next_enrolled_at"),
        F.count(F.lit(1)).over(w_course_id_student_id).alias("rows_in_course_id_student_id"),
        F.first("score").over(w_course_id_student_id_by_enro).alias("first_score"),
        F.last("course_credits", ignorenulls=True).over(w_course_id_by_score_running).alias("last_known_course_credits"),
        F.dense_rank().over(w_enrolled_at_month_by_enrolle).alias("enrolled_at_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_course_credits), 3).alias("course_credits_cume_dist"),
        F.size(F.collect_set("course_level").over(w_course_id_student_id)).alias("distinct_course_level_in_course_id_student_id"),
        F.rank().over(w_enrolled_at_month_by_enrolle).alias("enrolled_at_rank"),
        F.first("enrolled_at").over(w_enrolled_at_month_by_enrolle_2).alias("first_enrolled_at"),
        F.dense_rank().over(w_course_id_by_score).alias("score_dense_rank"),
        F.size(F.collect_set("grade_status").over(w_course_level_student_id)).alias("distinct_grade_status_in_course_level_student_id"),
        F.nth_value("enrolled_at", 2).over(w_course_level_course_id_by_co).alias("second_enrolled_at"),
    )
)

top_rows = (
    with_windows
    .filter((F.col("enrolled_at_dense_rank") == 1) & (F.col("second_enrolled_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp")))
)

result = (
    top_rows
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "grade_status_ref",
        "course_code",
        "course_credits",
        "second_course_credits",
        "next_enrolled_at",
        "rows_in_course_id_student_id",
        "enrolled_at_dense_rank",
        "course_credits_cume_dist",
        "distinct_course_level_in_course_id_student_id",
        "first_enrolled_at",
        "score_dense_rank",
        "distinct_grade_status_in_course_level_student_id",
        "second_enrolled_at",
        F.greatest(F.col("enrolled_at_dense_rank"), F.col("second_course_credits")).alias("greatest_enrolled_at_dense_rank_second_course_cr"),
    )
    .orderBy("course_code", F.col("second_course_credits").desc())
)
