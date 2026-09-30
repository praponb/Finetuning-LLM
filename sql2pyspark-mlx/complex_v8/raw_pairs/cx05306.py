from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_to_date_enrolled = Window.partitionBy(F.to_date("enrolled_at"), "enrolled_at_month")

scoped_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
    )
)

derived = (
    scoped_enrollments
    .filter(F.col("term_ref").isNotNull())
    .select(
        "*",
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
        (F.col("enrolled_at_month") + F.col("score")).alias("enrolled_at_month_plus_score"),
    )
)

filtered = (
    derived.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") < 76.8) & (F.col("e.student_id") >= 7), "left_anti")
    .filter(F.month(F.col("enrolled_at")).isin(1, 3))
)

with_trends = (
    filtered
    .withColumn("rows_in_enrolled_at_to_date_enrolled_at_month", F.count(F.lit(1)).over(w_enrolled_at_to_date_enrolled))
    .withColumn("enrolled_at_to_date_enrolled_at_month_avg_enroll", F.round(F.avg("enrolled_at_month").over(w_enrolled_at_to_date_enrolled), 2))
)

with_details_student = (
    with_trends.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.major").isNotNull()), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_month",
        "s.term_ref",
        "s.rows_in_enrolled_at_to_date_enrolled_at_month",
        F.col("st.is_international").alias("student_is_international"),
    )
)

labeled = (
    with_details_student
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "enrolled_at_month",
        "student_is_international",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

annotated = (
    labeled
    .filter(F.col("score") <= 56.1)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        (F.col("enrolled_at_month") + F.col("score")).alias("enrolled_at_month_plus_score"),
        F.round(F.col("score") / F.nullif(F.col("enrolled_at_month"), F.lit(0)), 3).alias("score_per_enrolled_at_month"),
    )
)

result = (
    annotated
    .filter(F.col("score").between(23.7, 57.8))
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at_month_plus_score",
        "score_per_enrolled_at_month",
        F.abs(F.col("score") - 38.3).alias("score_distance"),
    )
    .orderBy(
        F.col("id").desc(),
        F.col("enrolled_at_month_plus_score").desc_nulls_first(),
    )
)
