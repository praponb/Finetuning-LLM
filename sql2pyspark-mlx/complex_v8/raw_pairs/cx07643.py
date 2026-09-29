from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id")
w_term_by_enrolled_at_running = Window.partitionBy("term").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_min_enrollment_score = Window.orderBy(F.col("min_enrollment_score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_last_known_enrolled_a = Window.orderBy("last_known_enrolled_at", "id")
w_all_by_last_known_enrolled_a_2 = Window.orderBy("last_known_enrolled_at", "id").rowsBetween(-6, -1)

cleaned = (
    df_enrollments
    .filter((F.hour(F.col("enrolled_at")).between(9, 14)) & (F.col("student_id") <= 12))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.max("score").alias("max_enrollment_score"),
    )
)

with_counts = (
    cleaned.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.min_enrollment_score", "e.max_enrollment_score")
)

with_windows = (
    with_counts
    .select(
        "*",
        F.sum("score_bucket").over(w_student_id).alias("student_id_sum_score_bucket"),
        F.first("min_enrollment_score", ignorenulls=True).over(w_term_by_enrolled_at_running).alias("first_known_min_enrollment_score"),
        F.last("enrolled_at", ignorenulls=True).over(w_all_by_min_enrollment_score).alias("last_known_enrolled_at"),
    )
)

tagged = (
    with_windows
    .filter(F.col("score") >= 11.3)
    .select(
        "*",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket_2"),
        F.round((F.unix_timestamp(F.col("last_known_enrolled_at")) - F.unix_timestamp(F.col("enrolled_at"))) / 60, 1).alias("minutes_enrolled_at_to_last_known_enrolled_at"),
    )
)

sequenced = (
    tagged
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "enrolled_at",
        "score_bucket",
        "max_enrollment_score",
        "student_id_sum_score_bucket",
        "last_known_enrolled_at",
        "score_bucket_2",
        "minutes_enrolled_at_to_last_known_enrolled_at",
        F.lead("max_enrollment_score").over(w_all_by_last_known_enrolled_a).alias("next_max_enrollment_score"),
        F.rank().over(w_all_by_last_known_enrolled_a).alias("last_known_enrolled_at_rank"),
        F.sum("min_enrollment_score").over(w_all_by_last_known_enrolled_a_2).alias("min_enrollment_score_sum_6_prior"),
    )
)

with_parent = (
    sequenced.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.major").alias("student_major"),
        "st.student_name",
    )
)

top_rows = (
    with_parent
    .filter((F.col("last_known_enrolled_at_rank") == 1) & (F.col("student_major") != "CS"))
)

result = (
    top_rows
    .select(
        "id",
        "student_id",
        "score",
        "score_bucket",
        "max_enrollment_score",
        "student_id_sum_score_bucket",
        "last_known_enrolled_at",
        "score_bucket_2",
        "next_max_enrollment_score",
        "last_known_enrolled_at_rank",
        "min_enrollment_score_sum_6_prior",
        "student_is_international",
        "student_major",
        "student_name",
        (F.col("last_known_enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("last_known_enrolled_at_plus_30m"),
    )
)
