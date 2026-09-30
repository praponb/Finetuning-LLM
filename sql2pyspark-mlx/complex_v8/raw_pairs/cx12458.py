from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_credits_tier_by_credits = Window.partitionBy("credits_tier").orderBy(F.col("credits").desc(), "id")
w_enrollment_count_by_first_en = Window.partitionBy("enrollment_count").orderBy(F.col("first_enrollment_enrolled_at_2").desc(), "id")
w_dept_credits_quintile = Window.partitionBy("dept", "credits_quintile")
w_first_enrollment_enrolled_at = Window.partitionBy(F.to_date("first_enrollment_enrolled_at_2"), "dept").orderBy(F.col("prev_enrollment_count").desc(), "id")

base_courses = (
    df_courses
    .filter(F.col("credits") > 2)
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("level")), " ", "_")).alias("level_slug"),
        F.when(F.col("credits") >= 3, "A").when(F.col("credits") >= 2, "B").otherwise("C").alias("credits_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("course_code")), " ", "_")).alias("course_code_slug"),
    )
)

scored = (
    base_courses
    .select(
        "id",
        "dept",
        "credits",
        "level_slug",
        "credits_tier",
        "course_code_slug",
        F.lower(F.regexp_replace(F.trim(F.col("dept")), " ", "_")).alias("dept_slug"),
        F.coalesce(F.col("level"), F.lit("UNKNOWN")).alias("level_or_unknown"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    scored.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at", "e.first_enrollment_enrolled_at")
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at_2"),
        F.count("*").alias("enrollment_count"),
    )
)

summarized_rows_2 = (
    summarized_rows.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at_2",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

with_activity = (
    summarized_rows_2.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.student_id") <= 7), "left_semi")
)

sequenced = (
    with_activity
    .select(
        "id",
        "dept",
        "credits",
        "level_slug",
        "credits_tier",
        "course_code_slug",
        "dept_slug",
        "first_enrollment_enrolled_at_2",
        F.row_number().over(w_credits_tier_by_credits).alias("seq_no"),
        F.ntile(5).over(w_credits_tier_by_credits).alias("credits_quintile"),
        F.lag("enrollment_count").over(w_enrollment_count_by_first_en).alias("prev_enrollment_count"),
    )
)

windowed = (
    sequenced
    .select(
        "*",
        F.min("seq_no").over(w_dept_credits_quintile).alias("dept_credits_quintile_min_seq_no"),
        F.round(F.col("credits_quintile") - F.avg("credits_quintile").over(w_dept_credits_quintile), 2).alias("credits_quintile_vs_dept_credits_quintile_avg"),
        F.lead("prev_enrollment_count").over(w_first_enrollment_enrolled_at).alias("next_prev_enrollment_count"),
    )
)

result = (
    windowed
    .select(
        "id",
        "dept",
        "credits",
        "level_slug",
        "credits_tier",
        "course_code_slug",
        "dept_slug",
        "first_enrollment_enrolled_at_2",
        "seq_no",
        "credits_quintile",
        "prev_enrollment_count",
        "dept_credits_quintile_min_seq_no",
        "next_prev_enrollment_count",
    )
    .orderBy("level_slug", "credits_quintile")
)
