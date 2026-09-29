from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status")
w_all_by_score_ties = Window.orderBy("score")
w_all_by_score = Window.orderBy("score", "id")

recent_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
        (F.col("enrolled_at") + F.expr("INTERVAL 15 MINUTES")).alias("enrolled_at_plus_15m"),
    )
)

filtered = (
    recent_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status") != "FAIL"), "left_semi")
)

sequenced = (
    filtered
    .select(
        "*",
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.round(F.percent_rank().over(w_all_by_score_ties), 3).alias("score_percent_rank"),
        F.round((F.unix_timestamp("enrolled_at_plus_15m") - F.unix_timestamp(F.lag("enrolled_at_plus_15m").over(w_all_by_score))) / 60, 1).alias("minutes_since_prev_enrolled_at_plus_15m"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at_plus_15m",
        "rows_in_grade_status",
        "score_percent_rank",
        F.greatest(F.col("rows_in_grade_status"), F.col("score")).alias("greatest_rows_in_grade_status_score"),
    )
    .orderBy("course_id", F.col("rows_in_grade_status").desc())
)
