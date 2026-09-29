from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_hour = Window.partitionBy("enrolled_at_hour")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")

valid = (
    df_enrollments
    .filter((F.col("score") <= 57.6) & (F.to_date(F.col("enrolled_at")) < F.lit("2025-03-08").cast("date")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix"),
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at",
        "term_prefix",
        "enrolled_at_hour",
        F.size(F.collect_set("course_id").over(w_enrolled_at_hour)).alias("distinct_course_id_in_enrolled_at_hour"),
        F.round(F.col("enrolled_at_hour") - F.avg("enrolled_at_hour").over(w_enrolled_at_hour), 2).alias("enrolled_at_hour_vs_enrolled_at_hour_avg"),
        F.lag("enrolled_at").over(w_all_by_enrolled_at).alias("prev_enrolled_at"),
        F.count(F.lit(1)).over(w_enrolled_at_hour).alias("rows_in_enrolled_at_hour"),
    )
)

result = (
    metrics
    .cube("student_id", "enrolled_at_hour", "distinct_course_id_in_enrolled_at_hour")
    .agg(F.max("prev_enrolled_at").alias("last_prev_enrolled_at"))
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("enrolled_at_hour").asc_nulls_last(),
        F.col("distinct_course_id_in_enrolled_at_hour").asc_nulls_last(),
    )
)
