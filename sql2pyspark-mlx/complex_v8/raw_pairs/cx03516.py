from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("course_level", "student_id").orderBy(F.col("course_credits").desc(), F.col("id").desc())

filtered_enrollments = df_enrollments.filter(F.col("student_id").isNotNull())

term_stats = (
    filtered_enrollments
    .groupBy("term")
    .agg(F.percentile_approx("score", 0.5).alias("median_score"))
)

outliers = (
    filtered_enrollments.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.score") >= F.col("g.median_score"))
    .select(
        "s.*",
        "g.median_score",
        F.round(F.col("s.score") - F.col("g.median_score"), 2).alias("score_minus_median"),
    )
)

with_details = (
    outliers.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.median_score",
        "s.score_minus_median",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
        F.col("st.major").alias("student_major"),
        "st.student_name",
    )
)

unique_rows = (
    with_details
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "course_level",
        "course_credits",
        "student_major",
        "student_name",
        F.concat(F.col("student_major"), F.lit("#"), F.col("student_id").cast("string")).alias("student_major_ref"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
    .orderBy(F.col("student_major_ref").desc_nulls_first())
)
