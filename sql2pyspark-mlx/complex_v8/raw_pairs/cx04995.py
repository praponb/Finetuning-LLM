from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month_student_is = Window.partitionBy(F.month("enrolled_at"), "student_is_international").orderBy(F.col("score_filled").asc_nulls_last())
w_enrolled_at_month_student_is_2 = Window.partitionBy(F.month("enrolled_at"), "student_is_international").orderBy(F.col("score_filled").asc_nulls_last(), "id")

recent_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-19 15:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.when(F.col("term").isin("2025T2", "2025T1"), "group_a").when(F.col("term") == "2024T2", "group_b").otherwise("other").alias("term_group"),
    )
)

enriched_course = (
    recent_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        "s.score_filled",
        "s.term_group",
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

with_windows = (
    enriched_course
    .withColumn("score_filled_dense_rank", F.dense_rank().over(w_enrolled_at_month_student_is))
    .withColumn("rn", F.row_number().over(w_enrolled_at_month_student_is_2))
)

result = (
    with_windows
    .groupingSets(
        [["course_id", "term_group"], ["course_id"], []],
        "course_id", "term_group",
    )
    .agg(
        F.grouping("course_id").alias("is_total_course_id"),
        F.sum("rn").alias("total_rn"),
    )
    .select(
        "course_id",
        F.coalesce(F.col("term_group"), F.lit("ALL")).alias("term_group"),
        "is_total_course_id",
        "total_rn",
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("term_group").asc_nulls_last(),
    )
)
