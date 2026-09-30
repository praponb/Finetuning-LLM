from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_on_year_cnt_by_medi = Window.partitionBy("enrolled_on_year", "cnt").orderBy(F.col("median_max_enrollment_score").desc(), "enrolled_on_year")
w_enrolled_on_year_cnt_by_medi_2 = Window.partitionBy("enrolled_on_year", "cnt").orderBy(F.col("median_max_enrollment_score").desc(), "enrolled_on_year").rowsBetween(-3, -1)
w_enrolled_on_year_cnt_by_medi_3 = Window.partitionBy("enrolled_on_year", "cnt").orderBy(F.col("median_max_enrollment_score").desc())

valid_students = (
    df_students
    .filter((F.col("gpa") >= 2.7) & (F.col("cohort") == "C2024"))
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("score").alias("max_enrollment_score"))
)

with_stats = (
    valid_students.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select("s.*", "e.max_enrollment_score")
)

grouped = (
    with_stats
    .groupBy(F.year("enrolled_on").alias("enrolled_on_year"))
    .agg(
        F.max("enrolled_on").alias("last_enrolled_on"),
        F.round(F.avg("max_enrollment_score"), 2).alias("avg_max_enrollment_score"),
        F.percentile_approx("max_enrollment_score", 0.5).alias("median_max_enrollment_score"),
        F.count("*").alias("cnt"),
        F.sum("gpa").alias("total_gpa"),
    )
)

ranked = (
    grouped
    .select(
        "*",
        F.datediff(F.col("last_enrolled_on"), F.lag("last_enrolled_on").over(w_enrolled_on_year_cnt_by_medi)).alias("days_since_prev_last_enrolled_on"),
        F.sum("avg_max_enrollment_score").over(w_enrolled_on_year_cnt_by_medi_2).alias("avg_max_enrollment_score_sum_3_prior"),
        F.round(F.cume_dist().over(w_enrolled_on_year_cnt_by_medi_3), 3).alias("median_max_enrollment_score_cume_dist"),
    )
)

result = (
    ranked
    .select(
        "enrolled_on_year",
        "median_max_enrollment_score_cume_dist",
        "avg_max_enrollment_score",
    )
    .unpivot(
        ids=["enrolled_on_year"],
        values=["median_max_enrollment_score_cume_dist", "avg_max_enrollment_score"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
