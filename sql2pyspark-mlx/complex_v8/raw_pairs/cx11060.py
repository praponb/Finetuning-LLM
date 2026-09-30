from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_enrollment_term_2024t2_by = Window.partitionBy("n_enrollment_term_2024t2").orderBy("cnt", "enrollment_id").rowsBetween(-1, Window.currentRow)
w_all_by_score_distance_to_sco = Window.orderBy("score_distance_to_score_ratio")

prepared = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("enrolled_at") < F.lit("2025-02-24 14:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
        F.abs(F.col("score") - 50.9).alias("score_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

with_activity = (
    prepared.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.enrolled_at") < F.lit("2025-02-18 11:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("term_slug").isNotNull())
)

line_level = (
    with_activity.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id").isNotNull()), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.term_prefix",
        "s.score_distance",
        "s.term_slug",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
    )
)

per_group = (
    line_level
    .groupBy("enrollment_id")
    .agg(
        F.sum(F.when(F.col("enrollment_term").isin("2025T1", "2024T2"), F.col("score_distance"))).alias("score_distance_enrollment_term_in_2025t1_20"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("enrollment_term") == "2024T2", 1)).alias("n_enrollment_term_2024t2"),
        F.round(F.sum("score_distance") / F.nullif(F.sum("score"), F.lit(0)), 3).alias("score_distance_to_score_ratio"),
        F.avg("score_distance").alias("avg_score_distance_check"),
    )
    .filter(F.col("avg_score_distance_check") > 35.0)
    .drop("avg_score_distance_check")
)

ranked = (
    per_group
    .withColumn("n_enrollment_term_2024t2_max_2", F.max("n_enrollment_term_2024t2").over(w_n_enrollment_term_2024t2_by))
    .withColumn("score_distance_to_score_ratio_cume_dist", F.round(F.cume_dist().over(w_all_by_score_distance_to_sco), 3))
)

joined = (
    ranked.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("e.student_id"), "left")
    .select(
        "s.enrollment_id",
        "s.score_distance_enrollment_term_in_2025t1_20",
        "s.cnt",
        "s.n_enrollment_term_2024t2",
        "s.score_distance_to_score_ratio",
        "s.n_enrollment_term_2024t2_max_2",
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

result = (
    joined
    .select(
        "enrollment_id",
        "score_distance_enrollment_term_in_2025t1_20",
        "cnt",
        "score_distance_to_score_ratio",
        "n_enrollment_term_2024t2_max_2",
        "enrollment_score",
        "enrollment_term",
        "student_is_international",
        "student_cohort",
        F.coalesce(F.col("enrollment_score"), F.lit(0.0)).alias("enrollment_score_filled"),
        F.lower(F.trim(F.col("student_cohort"))).alias("student_cohort_clean"),
    )
)
