from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_course_ids = Window.partitionBy("distinct_course_ids")
w_all_by_distinct_course_ids = Window.orderBy("distinct_course_ids", "course_id", "student_id")
w_course_id_by_distinct_course = Window.partitionBy("course_id").orderBy(F.col("distinct_course_ids").asc_nulls_last(), "course_id", "student_id")

scoped = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("score") > 32.7))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 58.8, "A").when(F.col("score") >= 13.9, "B").otherwise("C").alias("score_tier"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

scored = (
    scoped
    .select(
        "*",
        F.concat(F.col("score_tier"), F.lit("#"), F.col("course_id").cast("string")).alias("score_tier_ref"),
        F.when(F.col("score_tier").isin("A", "C"), "group_a").when(F.col("score_tier") == "B", "group_b").otherwise("other").alias("score_tier_group"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
    )
)

filtered = (
    scored.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id") >= 19), "left_anti")
)

grouped = (
    filtered
    .groupBy("course_id", "student_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.round(100 * F.count(F.when(F.col("grade_status") != "WITHDRAWN", 1)) / F.count("*"), 1).alias("pct_grade_status_withdrawn"),
        F.sum(F.when(~F.col("term").isin("2025T1", "2024T2"), F.col("score"))).alias("score_term_not_in_2025t1_2024t2"),
        F.avg("score").alias("avg_score_check"),
    )
    .filter(F.col("avg_score_check") > 20.6)
    .drop("avg_score_check")
)

with_parent_student = (
    grouped.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.course_id",
        "s.student_id",
        "s.distinct_course_ids",
        "s.pct_grade_status_withdrawn",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("c.credits").alias("course_credits"),
    )
)

with_windows = (
    with_parent_student
    .select(
        "*",
        F.count(F.lit(1)).over(w_distinct_course_ids).alias("rows_in_distinct_course_ids"),
        F.row_number().over(w_all_by_distinct_course_ids).alias("rn"),
        (F.col("course_credits") - F.lag("course_credits").over(w_all_by_distinct_course_ids)).alias("course_credits_change"),
        F.sum("student_gpa").over(w_distinct_course_ids).alias("distinct_course_ids_sum_student_gpa"),
        F.lead("distinct_course_ids").over(w_course_id_by_distinct_course).alias("next_distinct_course_ids"),
    )
)

result = (
    with_windows
    .select(
        "course_id",
        "student_id",
        F.col("course_credits").alias("m_course_credits"),
        F.col("rows_in_distinct_course_ids").alias("rows_in_distinct_course_ids_v"),
        F.col("rn").alias("m_rn"),
    )
    .unpivot(
        ids=["course_id", "student_id"],
        values=["m_course_credits", "rows_in_distinct_course_ids_v", "m_rn"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
