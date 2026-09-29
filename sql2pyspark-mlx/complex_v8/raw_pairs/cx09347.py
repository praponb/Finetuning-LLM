from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("enrollment_term", "dept").orderBy("enrollment_enrolled_at", "id", "enrollment_id")

prepared = (
    df_courses
    .filter(F.col("level") != "core")
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.concat(F.col("course_code"), F.lit("#"), F.col("id").cast("string")).alias("course_code_ref"),
        F.abs(F.col("credits") - 3).alias("credits_distance"),
    )
)

line_level = (
    prepared.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.course_code",
        "s.dept",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

latest_per_key = (
    line_level
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .groupBy("enrollment_student_id")
    .pivot("dept", ["Biz", "Bio", "Art"])
    .agg(F.min("enrollment_score").alias("min_v"), F.sum("enrollment_score").alias("sum_v"))
    .orderBy("enrollment_student_id")
)
