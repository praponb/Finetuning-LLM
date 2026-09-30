from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter((F.col("grade_status") != "WITHDRAWN") & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.when(F.col("grade_status").isin("INCOMPLETE", "FAIL"), "group_a").when(F.col("grade_status") == "WITHDRAWN", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.upper(F.trim(F.col("st.student_name"))).like("J%")), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at_label",
        "s.enrolled_at_quarter",
        "s.grade_status_group",
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    with_parent
    .rollup("student_is_international", "grade_status_group")
    .agg(
        F.grouping("student_is_international").alias("is_total_student_is_international"),
        F.sum("enrolled_at_quarter").alias("total_enrolled_at_quarter"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("student_is_international").asc_nulls_last(),
        F.col("grade_status_group").asc_nulls_last(),
    )
)
