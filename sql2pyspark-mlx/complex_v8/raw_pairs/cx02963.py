from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_student_id_enrolled_at_year = Window.partitionBy("student_id", F.year("enrolled_at")).orderBy("enrolled_at", "id")
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_enrollments = (
    df_enrollments
    .filter((F.col("score") >= 11.1) | (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
        F.when(F.col("term").isin("2025T2", "2025T1"), "group_a").when(F.col("term") == "2024T2", "group_b").otherwise("other").alias("term_group"),
    )
)

enriched_course = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_ref",
        "s.term_group",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
    )
)

metrics = (
    enriched_course
    .select(
        "id",
        "course_id",
        "grade_status_ref",
        "course_credits",
        "course_dept",
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_course_id), F.lit(0)), 4).alias("course_credits_share_of_course_id"),
        F.ntile(10).over(w_all_by_enrolled_at).alias("enrolled_at_decile"),
        F.size(F.collect_set("course_dept").over(w_course_id)).alias("distinct_course_dept_in_course_id"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_id_enrolled_at_year))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.first("enrolled_at", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("first_known_enrolled_at"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

summarized_rows = (
    metrics.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

per_group = (
    summarized_rows
    .filter(F.col("minutes_since_prev_enrolled_at").between(1000.0, 4750.0))
    .groupBy("enrolled_at_decile", "course_dept", "course_id")
    .agg(
        F.array_sort(F.collect_set("course_dept")).alias("course_dept_list"),
        F.sum("minutes_since_prev_enrolled_at").alias("total_minutes_since_prev_enrolled_at"),
    )
)

result = (
    per_group
    .filter(F.col("total_minutes_since_prev_enrolled_at") > 13500.0)
    .select(
        "enrolled_at_decile",
        "course_dept",
        F.substring(F.trim(F.col("course_dept")), 1, 4).alias("course_dept_prefix"),
    )
    .orderBy(F.col("course_dept_prefix").asc_nulls_last())
)
