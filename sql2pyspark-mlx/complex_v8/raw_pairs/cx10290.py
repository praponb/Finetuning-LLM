from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_score_distance = Window.partitionBy("term").orderBy("score_distance", "id")
w_term = Window.partitionBy("term")
w_grade_status_enrolled_at_wee = Window.partitionBy("grade_status", F.year("enrolled_at_week"))
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at_week", "id")

scoped = (
    df_enrollments
    .filter((F.col("student_id") <= 2) | (F.col("score").between(17.4, 92.4)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 63.9).alias("score_distance"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

annotated = (
    scoped
    .select(
        "*",
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

with_trends = (
    annotated
    .select(
        "*",
        F.lead("score", 1, 0).over(w_term_by_score_distance).alias("next_score"),
        F.lead("score_distance", 2).over(w_term_by_score_distance).alias("next2_score_distance"),
        F.size(F.collect_set("grade_status").over(w_term)).alias("distinct_grade_status_in_term"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.min("days_since_enrolled_at").over(w_grade_status_enrolled_at_wee).alias("grade_status_enrolled_at_week_year_min_days_sinc"),
        F.round(F.col("next2_score_distance") / F.nullif(F.sum("next2_score_distance").over(w_grade_status_enrolled_at_wee), F.lit(0)), 4).alias("next2_score_distance_share_of_grade_status_enrol"),
        F.max("days_since_enrolled_at").over(w_grade_status_enrolled_at_wee).alias("grade_status_enrolled_at_week_year_max_days_sinc"),
        F.row_number().over(w_grade_status_by_enrolled_at).alias("enrolled_at_week_seq"),
        F.ntile(4).over(w_grade_status_by_enrolled_at).alias("enrolled_at_week_quartile"),
    )
)

annotated_2 = (
    metrics
    .filter(F.col("course_id") <= 8)
    .select(
        "*",
        F.abs(F.col("next_score") - 55.6).alias("next_score_distance"),
        F.least(F.col("score"), F.col("days_since_enrolled_at")).alias("least_score_days_since_enrolled_at"),
    )
)

totals = (
    annotated_2
    .groupBy("student_id", "term", F.to_date("enrolled_at").alias("enrolled_at_day"))
    .agg(
        F.percentile_approx("grade_status_enrolled_at_week_year_max_days_sinc", 0.5).alias("median_grade_status_enrolled_at_week_year_max_da"),
        F.max("next_score_distance").alias("max_next_score_distance"),
        F.max("least_score_days_since_enrolled_at").alias("max_least_score_days_since_enrolled_at"),
    )
)

screened = (
    totals.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.enrolled_on").between(F.lit("2024-04-01").cast("date"), F.lit("2025-06-27").cast("date"))) & (F.col("st.major").isin("CS", "Art", "Math")), "left_semi")
)

result = (
    screened
    .select(
        "student_id",
        "enrolled_at_day",
        "median_grade_status_enrolled_at_week_year_max_da",
        "max_next_score_distance",
        "max_least_score_days_since_enrolled_at",
    )
    .orderBy(F.col("max_next_score_distance").asc_nulls_last())
)
