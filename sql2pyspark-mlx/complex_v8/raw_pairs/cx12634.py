from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month_grade_stat = Window.partitionBy(F.month("enrolled_at"), "grade_status").orderBy("enrolled_at")
w_student_id_by_score_adj_runn = Window.partitionBy("student_id").orderBy("score_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_days_since_enrolled_a = Window.orderBy("days_since_enrolled_at", "id")
w_all_by_days_since_enrolled_a_2 = Window.orderBy("days_since_enrolled_at", "id").rowsBetween(-2, -1)
w_grade_status = Window.partitionBy("grade_status")

valid = (
    df_enrollments
    .filter((F.col("score") < 63.9) & (F.col("term") == "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at_2"),
        F.round(F.col("score") * 1.5, 2).alias("score_adj"),
    )
)

sequenced = (
    valid
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrolled_at",
        "days_since_enrolled_at_2",
        "score_adj",
        F.round(F.percent_rank().over(w_enrolled_at_month_grade_stat), 3).alias("enrolled_at_percent_rank"),
        F.sum("score_adj").over(w_student_id_by_score_adj_runn).alias("running_score_adj"),
        F.row_number().over(w_all_by_days_since_enrolled_a).alias("days_since_enrolled_at_seq"),
        F.round(F.avg("score_adj").over(w_all_by_days_since_enrolled_a_2), 2).alias("score_adj_avg_2_prior"),
        F.size(F.collect_set("student_id").over(w_grade_status)).alias("distinct_student_id_in_grade_status"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.min("score").alias("min_enrollment_score"))
)

with_stats = (
    sequenced.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.min_enrollment_score")
)

result = (
    with_stats
    .select(
        "id",
        "score_adj",
        "score_adj_avg_2_prior",
        "running_score_adj",
    )
    .unpivot(
        ids=["id"],
        values=["score_adj", "score_adj_avg_2_prior", "running_score_adj"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
