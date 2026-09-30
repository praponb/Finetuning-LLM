from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dept_by_enrollment_score = Window.partitionBy("dept").orderBy(F.col("enrollment_score").desc_nulls_first(), "id", "enrollment_id")
w_enrollment_term = Window.partitionBy("enrollment_term")

prepared_courses = (
    df_courses
    .filter((F.col("dept") == "Biz") | (F.lower(F.col("course_code")).like("%ar%")))
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        F.concat_ws(": ", F.col("level"), F.col("course_code")).alias("level_course_code_label"),
        F.lower(F.regexp_replace(F.trim(F.col("dept")), " ", "_")).alias("dept_slug"),
    )
)

exploded_detail = (
    prepared_courses.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.course_code",
        "s.dept",
        "s.level_course_code_label",
        "s.dept_slug",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
    )
)

with_trends = (
    exploded_detail
    .select(
        "id",
        "course_code",
        "level_course_code_label",
        "enrollment_id",
        "enrollment_term",
        F.round(F.cume_dist().over(w_dept_by_enrollment_score), 3).alias("enrollment_score_cume_dist"),
        F.round(F.col("enrollment_score") / F.nullif(F.sum("enrollment_score").over(w_enrollment_term), F.lit(0)), 4).alias("enrollment_score_share_of_enrollment_term"),
    )
)

result = (
    with_trends
    .filter(F.col("enrollment_id") >= 60)
    .select(
        "id",
        "course_code",
        "level_course_code_label",
        "enrollment_id",
        "enrollment_term",
        F.coalesce(F.col("enrollment_term"), F.lit("UNKNOWN")).alias("enrollment_term_or_unknown"),
    )
    .orderBy(F.col("enrollment_id").asc())
)
