from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_prior_1 = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-1, -1)
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("term", "student_id").orderBy(F.col("last_score_filled").desc(), F.col("id").desc())

filtered_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") != "PASS") & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.concat_ws(" / ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
    )
)

summarized_rows = (
    filtered_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.min_enrollment_score", "e.avg_enrollment_score")
)

with_windows = (
    summarized_rows
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "score_filled",
        "term_grade_status_label",
        "grade_status_slug",
        "avg_enrollment_score",
        F.lead("score_filled").over(w_all_by_enrolled_at).alias("next_score_filled"),
        F.lag("score_filled").over(w_all_by_enrolled_at).alias("prev_score_filled"),
        F.last("score_filled").over(w_all_by_enrolled_at_full).alias("last_score_filled"),
        F.max("min_enrollment_score").over(w_all_by_enrolled_at_prior_1).alias("min_enrollment_score_max_1_prior"),
        F.first("score", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("first_known_score"),
    )
)

with_activity = (
    with_windows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id").isNotNull()), "left_anti")
)

with_activity_2 = (
    with_activity.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id") <= 11) & (F.col("e.term") == "2024T1"), "left_semi")
    .filter(F.col("term").isin("2025T2", "2024T1", "2025T1"))
)

latest = (
    with_activity_2
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "score_filled",
        "term_grade_status_label",
        "grade_status_slug",
        "avg_enrollment_score",
        "next_score_filled",
        "prev_score_filled",
        "last_score_filled",
        "min_enrollment_score_max_1_prior",
    )
    .orderBy(F.col("min_enrollment_score_max_1_prior").desc())
)
