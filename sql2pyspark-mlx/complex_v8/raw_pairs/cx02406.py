from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("enrollment_term", "enrollment_student_id").orderBy("enrollment_score", "id", "enrollment_id")

filtered = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("enrolled_at") < F.lit("2025-02-18 11:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

with_children_enrollments = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

enriched = (
    with_children_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "enrollment_id",
        "enrollment_student_id",
        "enrollment_term",
        "enrollment_score",
        "enrollment_grade_status",
        F.length(F.trim(F.col("enrollment_term"))).alias("enrollment_term_len"),
        F.concat(F.col("enrollment_term"), F.lit("#"), F.col("enrollment_id").cast("string")).alias("enrollment_term_ref"),
        (F.floor(F.col("enrollment_score") / 25) * 25).alias("enrollment_score_bucket"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupingSets(
        [["enrollment_term", "student_id"], ["enrollment_term"], ["student_id"], []],
        "enrollment_term", "student_id",
    )
    .agg(
        F.min("enrollment_term_len").alias("min_enrollment_term_len"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("enrollment_term").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
