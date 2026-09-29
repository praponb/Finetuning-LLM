from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_all_by_enrolled_at_quarter = Window.orderBy(F.col("enrolled_at_quarter").desc(), "id")
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_enrollments
    .filter((F.col("student_id") <= 6) & (F.col("grade_status") == "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

filtered = (
    cleaned.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.enrolled_at") >= F.lit("2025-01-07 10:00:00").cast("timestamp")), "left_semi")
)

windowed = (
    filtered
    .select(
        "*",
        F.rank().over(w_all_by_enrolled_at).alias("enrolled_at_rank"),
        (F.col("grade_status_len") - F.lag("grade_status_len").over(w_all_by_enrolled_at)).alias("grade_status_len_change"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_quarter), 3).alias("enrolled_at_quarter_percent_rank"),
        F.sum("score").over(w_all_by_enrolled_at_running).alias("running_score"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "enrolled_at_quarter",
        "enrolled_at_rank",
        "grade_status_len_change",
        "enrolled_at_quarter_percent_rank",
        "running_score",
        F.coalesce(F.col("enrolled_at_rank"), F.lit(0)).alias("enrolled_at_rank_filled"),
    )
    .orderBy(
        F.col("score").asc_nulls_last(),
        F.col("grade_status_len_change").asc_nulls_last(),
    )
)
