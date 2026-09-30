from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_month_fil = Window.orderBy("enrolled_at_month_filled", "id")
w_all_by_term_len = Window.orderBy(F.col("term_len").desc_nulls_first(), "id")
w_enrolled_at_month_by_enrolle = Window.partitionBy("enrolled_at_month").orderBy("enrolled_at", "id").rowsBetween(-2, -1)
w_latest = Window.partitionBy("student_id", "grade_status_or_unknown").orderBy(F.col("score").desc(), F.col("id").desc())

cleaned = (
    df_enrollments
    .filter(F.col("grade_status").isin("WITHDRAWN", "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.coalesce(F.col("enrolled_at_month"), F.lit(0)).alias("enrolled_at_month_filled"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.abs(F.col("enrolled_at_quarter") - 3).alias("enrolled_at_quarter_distance"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.ntile(3).over(w_all_by_enrolled_at_month_fil).alias("enrolled_at_month_filled_tercile"),
        F.lead("enrolled_at", 2).over(w_all_by_term_len).alias("next2_enrolled_at"),
        F.sum("enrolled_at_quarter").over(w_enrolled_at_month_by_enrolle).alias("enrolled_at_quarter_sum_2_prior"),
    )
)

with_activity = (
    windowed
    .crossJoin(windowed.agg(F.avg("enrolled_at_month_filled_tercile").alias("overall_avg_enrolled_at_month_filled_tercile")))
    .filter(F.col("enrolled_at_month_filled_tercile") > F.col("overall_avg_enrolled_at_month_filled_tercile"))
    .drop("overall_avg_enrolled_at_month_filled_tercile")
)

scored = (
    with_activity
    .select(
        "*",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

unique_rows = (
    scored
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "enrolled_at_quarter",
        "term_len",
        "enrolled_at_month_filled",
        "enrolled_at_label",
        "enrolled_at_quarter_distance",
        "enrolled_at_month_filled_tercile",
        "next2_enrolled_at",
        "enrolled_at_quarter_sum_2_prior",
        "grade_status_or_unknown",
        F.coalesce(F.col("grade_status_or_unknown"), F.lit("UNKNOWN")).alias("grade_status_or_unknown_or_unknown"),
    )
    .orderBy(F.col("enrolled_at_quarter").asc(), "id")
    .limit(5)
)
