from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("enrollment_course_id", "major").orderBy(F.col("gpa_filled").desc(), F.col("id").desc(), F.col("enrollment_id").desc())

cleaned = (
    df_students
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        F.coalesce(F.col("gpa"), F.lit(0.0)).alias("gpa_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("student_name")), " ", "_")).alias("student_name_slug"),
        F.date_add(F.col("enrolled_on"), 30).alias("enrolled_on_plus_30d"),
    )
)

line_level = (
    cleaned.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.cohort",
        "s.major",
        "s.gpa_filled",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

latest_per_key = (
    line_level
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    latest_per_key.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.enrollment_course_id")) & (F.col("e.score") <= 77.0) & (F.col("e.student_id") >= 15), "left_anti")
)

result = (
    filtered
    .cube("enrollment_id", "cohort", "major")
    .agg(
        F.max("gpa_filled").alias("max_gpa_filled"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .select(
        "enrollment_id",
        F.coalesce(F.col("cohort"), F.lit("ALL")).alias("cohort"),
        F.coalesce(F.col("major"), F.lit("ALL")).alias("major"),
        "max_gpa_filled",
        "distinct_ids",
    )
    .orderBy(
        F.col("enrollment_id").asc_nulls_last(),
        F.col("cohort").asc_nulls_last(),
        F.col("major").asc_nulls_last(),
    )
)
