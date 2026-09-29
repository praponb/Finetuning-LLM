from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_term_in_2024t2_2025t2_2025 = Window.partitionBy("n_term_in_2024t2_2025t2_2025t1", "grade_status")
w_all_by_max_score_running = Window.orderBy(F.col("max_score").desc(), "course_id", "grade_status").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_enrollments = (
    df_enrollments
    .filter((F.month(F.col("enrolled_at")).isin(1, 2)) & (F.col("grade_status").isin("FAIL", "WITHDRAWN")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

totals = (
    filtered_enrollments
    .filter(F.col("term").isNotNull())
    .groupBy("course_id", "grade_status")
    .agg(
        F.sum(F.when(F.col("term").isin("2024T2", "2025T2", "2025T1"), 1).otherwise(0)).alias("n_term_in_2024t2_2025t2_2025t1"),
        F.max("score").alias("max_score"),
    )
)

combined_details_course = (
    totals.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.course_id",
        "s.grade_status",
        "s.n_term_in_2024t2_2025t2_2025t1",
        "s.max_score",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
        "c.course_code",
    )
)

metrics = (
    combined_details_course
    .select(
        "*",
        F.sum("course_credits").over(w_n_term_in_2024t2_2025t2_2025).alias("n_term_in_2024t2_2025t2_2025t1_grade_status_sum_"),
        F.count(F.lit(1)).over(w_all_by_max_score_running).alias("running_count"),
        F.first("max_score", ignorenulls=True).over(w_all_by_max_score_running).alias("first_known_max_score"),
        F.round(F.col("course_credits") - F.avg("course_credits").over(w_n_term_in_2024t2_2025t2_2025), 2).alias("course_credits_vs_n_term_in_2024t2_2025t2_2025t1"),
    )
)

combined_details_course_2 = (
    metrics.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.level") == "advanced"), "left")
    .select("s.*", F.col("c.course_code").alias("course_course_code"))
)

with_details = (
    combined_details_course_2.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.col("c.credits").alias("course_credits_2"),
        F.col("c.course_code").alias("course_course_code_2"),
        F.col("c.level").alias("course_level_2"),
    )
)

result = (
    with_details
    .groupBy("course_level_2", "course_id")
    .pivot("grade_status", ["PASS", "WITHDRAWN", "INCOMPLETE", "FAIL"])
    .agg(F.min("n_term_in_2024t2_2025t2_2025t1_grade_status_sum_").alias("min_v"), F.count("n_term_in_2024t2_2025t2_2025t1_grade_status_sum_").alias("n"))
    .orderBy("course_level_2", "course_id")
)
