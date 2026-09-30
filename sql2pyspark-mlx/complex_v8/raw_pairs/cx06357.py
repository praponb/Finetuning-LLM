from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_student_id_term_by_days_sinc = Window.partitionBy("student_id", "term").orderBy(F.col("days_since_enrolled_at").desc(), "id")

valid = (
    df_enrollments
    .filter((F.col("student_id") <= 19) & (F.month(F.col("enrolled_at")).isin(1, 2)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

with_activity = (
    valid.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.enrolled_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")) & (F.col("e.score") < 85.8), "left_anti")
)

ranked = (
    with_activity
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.round(F.percent_rank().over(w_student_id_term_by_days_sinc), 3).alias("days_since_enrolled_at_percent_rank"),
    )
)

grade_status_stats = (
    ranked
    .groupBy("grade_status")
    .agg(F.max("score").alias("max_score"))
)

above_average = (
    ranked.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.score") == F.col("g.max_score"))
    .select(
        "s.*",
        "g.max_score",
        F.round(F.col("s.score") - F.col("g.max_score"), 2).alias("score_minus_max"),
    )
)

result = (
    above_average
    .select(
        "id",
        "days_since_enrolled_at_percent_rank",
        "max_score",
        "score_minus_max",
        "score",
    )
    .unpivot(
        ids=["id"],
        values=["days_since_enrolled_at_percent_rank", "max_score", "score_minus_max", "score"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
