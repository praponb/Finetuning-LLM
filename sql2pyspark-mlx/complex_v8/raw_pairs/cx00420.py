from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_enrolled_at_month = Window.partitionBy("course_id", F.month("enrolled_at"))
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_enrolled_at_2 = Window.orderBy("enrolled_at", "id")
w_all_by_enrolled_at_prior_6 = Window.orderBy("enrolled_at", "id").rowsBetween(-6, -1)

prepared_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp")) & (F.col("student_id") >= 16))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

labeled = (
    prepared_enrollments
    .withColumn("score_tier", F.when(F.col("score") >= 83.1, "large").when(F.col("score") >= 11.3, "medium").otherwise("small"))
)

windowed = (
    labeled
    .select(
        "*",
        F.size(F.collect_set("score_tier").over(w_course_id_enrolled_at_month)).alias("distinct_score_tier_in_course_id_enrolled_at_mon"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at), 3).alias("enrolled_at_percent_rank"),
        F.rank().over(w_all_by_enrolled_at_2).alias("enrolled_at_rank"),
        F.row_number().over(w_all_by_enrolled_at).alias("seq_no"),
        F.round(F.avg("score").over(w_all_by_enrolled_at_prior_6), 2).alias("score_avg_6_prior"),
    )
)

filtered = (
    windowed.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.enrolled_at") < F.lit("2025-02-24 13:00:00").cast("timestamp")) & (F.col("e.course_id") >= 6), "left_semi")
)

annotated = (
    filtered
    .filter(F.col("enrolled_at_rank") < 8)
    .select(
        "*",
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
        F.coalesce(F.col("seq_no"), F.lit(0)).alias("seq_no_filled"),
        F.length(F.trim(F.col("score_tier"))).alias("score_tier_len"),
    )
)

enriched_course = (
    annotated.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(~F.col("c.dept").isin("CS", "Math"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_percent_rank",
        "s.enrolled_at_rank",
        "s.seq_no",
        "s.enrolled_at_month",
        "s.seq_no_filled",
        "c.course_code",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
    )
)

result = (
    enriched_course
    .filter(F.col("enrolled_at_percent_rank") >= 0.1)
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "enrolled_at_percent_rank",
        "enrolled_at_rank",
        "seq_no",
        "course_level",
    )
    .orderBy(F.col("score").desc())
)
