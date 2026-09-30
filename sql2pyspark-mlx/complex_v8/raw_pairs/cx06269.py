from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("term", "grade_status").orderBy(F.col("enrolled_at_week").desc(), F.col("id").desc())

scoped = (
    df_enrollments
    .filter(F.col("student_id") >= 11)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.when(F.col("grade_status").isin("INCOMPLETE", "FAIL"), "group_a").when(F.col("grade_status") == "WITHDRAWN", "group_b").otherwise("other").alias("grade_status_group"),
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

derived = (
    scoped
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "grade_status_group",
        "enrolled_at_week",
        F.year(F.col("enrolled_at_week")).alias("enrolled_at_week_year"),
    )
)

labeled = (
    derived
    .filter(F.col("course_id").isNotNull())
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.initcap(F.trim(F.col("grade_status_group"))).alias("grade_status_group_clean"),
    )
)

latest_per_key = (
    labeled
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .groupBy("grade_status_group", "term")
    .pivot("grade_status", ["FAIL", "INCOMPLETE", "PASS"])
    .agg(F.count("enrolled_at_week_year"))
    .orderBy("grade_status_group", "term")
)
