from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_len_enrollment_term_by = Window.partitionBy("level_len", "enrollment_term").orderBy(F.col("enrollment_score").desc(), "id", "enrollment_id")
w_level_len_enrollment_term_by_2 = Window.partitionBy("level_len", "enrollment_term").orderBy(F.col("enrollment_score").desc(), "id", "enrollment_id").rowsBetween(-6, -1)

base_courses = (
    df_courses
    .select(
        "id",
        "course_code",
        "dept",
        "level",
    )
)

derived = (
    base_courses
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        F.length(F.trim(F.col("level"))).alias("level_len"),
    )
)

annotated = (
    derived
    .select(
        "*",
        F.concat(F.col("level"), F.lit("#"), F.col("id").cast("string")).alias("level_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("dept")), " ", "_")).alias("dept_slug"),
    )
)

detail_rows = (
    annotated.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.level_len",
        "s.level_ref",
        "s.dept_slug",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.term").alias("enrollment_term"),
    )
)

with_windows = (
    detail_rows
    .select(
        "id",
        "level_len",
        "level_ref",
        "dept_slug",
        "enrollment_id",
        "enrollment_score",
        "enrollment_grade_status",
        "enrollment_student_id",
        "enrollment_term",
        (F.col("level_len") - F.lag("level_len").over(w_level_len_enrollment_term_by)).alias("level_len_change"),
        F.sum("level_len").over(w_level_len_enrollment_term_by_2).alias("level_len_sum_6_prior"),
        F.ntile(5).over(w_level_len_enrollment_term_by).alias("enrollment_score_quintile"),
    )
)

result = (
    with_windows
    .filter(F.col("enrollment_id") <= 79)
    .select(
        "id",
        "level_len",
        "level_ref",
        "dept_slug",
        "enrollment_id",
        "enrollment_score",
        "enrollment_student_id",
        "level_len_change",
        "level_len_sum_6_prior",
        F.round(F.col("enrollment_score") / F.nullif(F.col("level_len_change"), F.lit(0)), 3).alias("enrollment_score_per_level_len_change"),
        F.when(F.col("enrollment_term").isin("2024T1", "2024T2"), "group_a").when(F.col("enrollment_term") == "2025T2", "group_b").otherwise("other").alias("enrollment_term_group"),
    )
)
