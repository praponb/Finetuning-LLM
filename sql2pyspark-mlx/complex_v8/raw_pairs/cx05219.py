from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_week_prio = Window.orderBy(F.col("enrolled_at_week").desc(), "id").rowsBetween(-1, -1)
w_course_id_enrolled_at_month = Window.partitionBy("course_id", F.month("enrolled_at")).orderBy("student_gpa")

filtered_enrollments = (
    df_enrollments
    .filter(F.month(F.col("enrolled_at")).isin(1, 2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

joined = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.upper(F.trim(F.col("st.student_name"))).like("B%"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        F.col("st.gpa").alias("student_gpa"),
        "st.student_name",
    )
)

filtered = (
    joined.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.grade_status").isNotNull()), "left_semi")
)

enriched = (
    filtered
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "student_gpa",
        "student_name",
        "enrolled_at_week",
        F.sum("student_gpa").over(w_all_by_enrolled_at_week_prio).alias("student_gpa_sum_1_prior"),
        F.round(F.percent_rank().over(w_course_id_enrolled_at_month), 3).alias("student_gpa_percent_rank"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

profiled = (
    with_trends.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

profiled_2 = (
    profiled.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.avg_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    profiled_2
    .select("id", "student_gpa_percent_rank", "student_gpa_sum_1_prior")
    .unpivot(
        ids=["id"],
        values=["student_gpa_percent_rank", "student_gpa_sum_1_prior"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
