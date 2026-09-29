from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("course_dept", "course_id").orderBy("enrolled_at", "id")

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("term").isin("2025T2", "2025T1", "2024T1")) | (F.col("grade_status") == "WITHDRAWN"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
    )
)

with_parent = (
    cleaned_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.level").isin("advanced", "intro")), "inner")
    .filter(F.col("c.dept") == "Biz")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_adj",
        "s.term_ref",
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("score").alias("max_enrollment_score"))
)

summarized_rows = (
    with_parent.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.max_enrollment_score")
)

latest_per_key = (
    summarized_rows
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .cube("term", "student_id")
    .agg(
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.count(F.when(F.col("grade_status").isin("PASS", "FAIL"), 1)).alias("n_grade_status_in_pass_fail"),
        F.countDistinct("term").alias("distinct_terms"),
    )
)
