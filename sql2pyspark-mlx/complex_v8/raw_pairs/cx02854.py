from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter(F.col("score") < 57.3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

enriched_student = (
    base_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        F.col("st.is_international").alias("student_is_international"),
        "st.student_name",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("c.credits").alias("course_credits"),
    )
)

stats = (
    enriched_student
    .groupBy(
        "course_id",
        "student_is_international",
        F.year("student_enrolled_on").alias("student_enrolled_on_year"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    stats
    .groupingSets(
        [["distinct_student_ids", "student_is_international", "course_id"], ["distinct_student_ids"], ["student_is_international"], []],
        "distinct_student_ids", "student_is_international", "course_id",
    )
    .agg(
        F.grouping("distinct_student_ids").alias("is_total_distinct_student_ids"),
        F.sum(F.when(F.col("student_is_international"), 1).otherwise(0)).alias("n_student_is_international_tru"),
        F.sum("distinct_ids").alias("total_distinct_ids"),
        F.round(F.avg("student_enrolled_on_year"), 2).alias("avg_student_enrolled_on_year"),
    )
)
