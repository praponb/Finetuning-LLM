from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at_year", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

with_prev = cleaned.withColumn("prev_enrolled_at_year", F.lag("enrolled_at_year").over(w_term))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at_year").isNull() | ((F.unix_timestamp("enrolled_at_year") - F.unix_timestamp("prev_enrolled_at_year")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_term_running))

result = (
    sessionized
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at_year",
        "prev_enrolled_at_year",
        "new_session_flag",
        "session_no",
        F.abs(F.col("session_no") - 4).alias("session_no_distance"),
        F.dayofweek(F.col("enrolled_at_year")).alias("enrolled_at_year_dayofweek"),
    )
    .orderBy(F.col("enrolled_at_year").desc_nulls_first())
)
