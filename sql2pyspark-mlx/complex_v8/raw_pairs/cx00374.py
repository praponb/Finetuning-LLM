from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_enrolled_at_ties = Window.partitionBy("term").orderBy("enrolled_at")
w_term_by_enrolled_at = Window.partitionBy("term").orderBy("enrolled_at", "id")

valid_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

with_trends = (
    valid_enrollments
    .select(
        "*",
        F.round(F.cume_dist().over(w_term_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist"),
        F.row_number().over(w_term_by_enrolled_at).alias("rn"),
    )
)

student_id_stats = (
    with_trends
    .groupBy("student_id")
    .agg(F.avg("rn").alias("avg_rn"))
)

above_average = (
    with_trends.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.rn") > F.col("g.avg_rn"))
    .select(
        "s.*",
        "g.avg_rn",
        F.round(F.col("s.rn") - F.col("g.avg_rn"), 2).alias("rn_minus_avg"),
    )
)

result = (
    above_average
    .cube("student_id", "grade_status", "course_id")
    .agg(
        F.grouping("student_id").alias("is_total_student_id"),
        F.round(F.avg("enrolled_at_cume_dist"), 2).alias("avg_enrolled_at_cume_dist"),
        F.count(F.when(~F.col("grade_status").isin("FAIL", "PASS"), 1)).alias("n_grade_status_not_in_fail_pas"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
