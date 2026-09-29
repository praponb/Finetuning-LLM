from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("grade_status", "term").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_all_by_enrolled_at_2 = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_row_count_full = Window.orderBy(F.col("row_count").asc_nulls_last(), "term", "student_id", "enrolled_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_row_count_running = Window.orderBy(F.col("row_count").desc(), "term", "student_id", "enrolled_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_row_count = Window.partitionBy("row_count")
w_all_by_row_count = Window.orderBy(F.col("row_count").asc_nulls_last(), "term", "student_id", "enrolled_at_month")

scoped_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

latest = (
    scoped_enrollments
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_parent_course = (
    latest.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_label",
        F.col("c.level").alias("course_level"),
    )
)

with_trends = (
    with_parent_course
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "course_level",
        F.first("enrolled_at", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("first_known_enrolled_at"),
        F.lead("enrolled_at", 2).over(w_all_by_enrolled_at).alias("next2_enrolled_at"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at_2))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
    )
)

rollup_base = (
    with_trends
    .filter(F.to_date(F.col("first_known_enrolled_at")) < F.lit("2025-02-24").cast("date"))
    .groupBy(
        "term",
        "student_id",
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
    )
    .agg(
        F.array_sort(F.collect_set("course_level")).alias("course_level_list"),
        F.count("*").alias("row_count"),
        F.min("next2_enrolled_at").alias("first_next2_enrolled_at"),
        F.sum(F.when(F.col("term").isin("2024T2", "2025T1", "2025T2"), 1).otherwise(0)).alias("n_term_in_2024t2_2025t1_2025t2"),
    )
    .filter(F.col("row_count") >= 3)
)

metrics = (
    rollup_base
    .select(
        "*",
        F.nth_value("row_count", 2).over(w_all_by_row_count_full).alias("second_row_count"),
        F.sum("row_count").over(w_all_by_row_count_running).alias("running_row_count"),
        F.max("row_count").over(w_row_count).alias("row_count_max_row_count"),
        F.round(F.col("row_count") / F.nullif(F.sum("row_count").over(w_row_count), F.lit(0)), 4).alias("row_count_share_of_row_count"),
        F.lag("enrolled_at_month").over(w_all_by_row_count).alias("prev_enrolled_at_month"),
        F.last("row_count").over(w_all_by_row_count_full).alias("last_row_count"),
    )
)

screened = (
    metrics
    .crossJoin(metrics.agg(F.min("second_row_count").alias("overall_min_second_row_count")))
    .filter(F.col("second_row_count") > F.col("overall_min_second_row_count"))
    .drop("overall_min_second_row_count")
)

result = (
    screened
    .select(
        "term",
        "student_id",
        "enrolled_at_month",
        "row_count",
        "first_next2_enrolled_at",
        "second_row_count",
        "running_row_count",
        "row_count_max_row_count",
        "row_count_share_of_row_count",
        "prev_enrolled_at_month",
        "last_row_count",
    )
    .orderBy(
        F.col("term").desc(),
        F.col("running_row_count").desc(),
        "student_id",
        "enrolled_at_month",
    )
    .limit(5)
)
