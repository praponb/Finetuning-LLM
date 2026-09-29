from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id")
w_term_by_enrolled_at_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_by_enrolled_at = Window.partitionBy("term").orderBy("enrolled_at", "id")

base_enrollments = (
    df_enrollments
    .filter(~F.col("term").isin("2025T2", "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
    )
)

enriched_course = (
    base_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_prefix",
        "s.score_adj",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

with_windows = (
    enriched_course
    .select(
        "*",
        F.round(F.col("student_gpa") / F.nullif(F.sum("student_gpa").over(w_student_id), F.lit(0)), 4).alias("student_gpa_share_of_student_id"),
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_student_id), F.lit(0)), 4).alias("course_credits_share_of_student_id"),
        F.max("student_gpa").over(w_term_by_enrolled_at_running).alias("max_student_gpa_so_far"),
        F.size(F.collect_set("course_id").over(w_student_id)).alias("distinct_course_id_in_student_id"),
        F.min("student_gpa").over(w_student_id).alias("student_id_min_student_gpa"),
        F.lag("student_gpa").over(w_term_by_enrolled_at).alias("prev_student_gpa"),
    )
)

grouped = (
    with_windows
    .groupBy("term", "course_credits", "student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.max("course_credits").alias("max_course_credits"),
        F.array_sort(F.collect_set("student_cohort")).alias("student_cohort_list"),
        F.count("*").alias("row_count"),
        F.min("enrolled_at").alias("first_enrolled_at"),
    )
)

scored = (
    grouped
    .select(
        "*",
        F.when(F.col("course_credits") >= 3, "large").when(F.col("course_credits") >= 2, "medium").otherwise("small").alias("course_credits_tier"),
        F.round((F.unix_timestamp(F.col("last_enrolled_at")) - F.unix_timestamp(F.col("first_enrolled_at"))) / 60, 1).alias("minutes_first_enrolled_at_to_last_enrolled_at"),
        F.coalesce(F.col("course_credits"), F.lit(0)).alias("course_credits_filled"),
    )
)

derived = (
    scored
    .select(
        "*",
        F.substring(F.trim(F.col("course_credits_tier")), 1, 3).alias("course_credits_tier_prefix"),
    )
)

labeled = (
    derived
    .select(
        "term",
        "course_credits",
        "student_id",
        "last_enrolled_at",
        "distinct_course_ids",
        "max_course_credits",
        "student_cohort_list",
        "row_count",
        "course_credits_tier",
        "minutes_first_enrolled_at_to_last_enrolled_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_enrolled_at")).alias("days_since_last_enrolled_at"),
        F.quarter(F.col("first_enrolled_at")).alias("first_enrolled_at_quarter"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

result = (
    labeled
    .filter(F.col("distinct_course_ids") < 8)
    .select(
        "term",
        "course_credits",
        "max_course_credits",
        "student_cohort_list",
        "row_count",
        "minutes_first_enrolled_at_to_last_enrolled_at",
        "days_since_last_enrolled_at",
        "first_enrolled_at_quarter",
        "term_slug",
    )
    .orderBy(
        F.col("course_credits").asc(),
        F.col("first_enrolled_at_quarter").desc_nulls_first(),
    )
)
