from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("course_id", "grade_status").orderBy(F.col("score").desc(), F.col("id").desc())

prepared_enrollments = (
    df_enrollments
    .filter(F.col("student_id") <= 13)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

deduped = (
    prepared_enrollments
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

totals = (
    deduped
    .filter(F.col("grade_status") == "FAIL")
    .groupBy("course_id")
    .agg(
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.count("*").alias("cnt"),
        F.round(100 * F.count(F.when(F.col("grade_status") == "FAIL", 1)) / F.count("*"), 1).alias("pct_grade_status_fail"),
        F.round(100 * F.count(F.when(F.col("grade_status").isin("FAIL", "PASS"), 1)) / F.count("*"), 1).alias("pct_grade_status_in_fail_pass"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("cnt") >= 3)
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("grade_status") == "PASS", 1)).alias("enrollment_grade_status_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

with_stats = (
    totals.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        "e.first_enrollment_enrolled_at",
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
    )
)

with_stats_2 = (
    with_stats.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

result = (
    with_stats_2
    .groupingSets(
        [["distinct_ids", "enrollment_grade_status_count"], ["distinct_ids"], ["enrollment_grade_status_count"], []],
        "distinct_ids", "enrollment_grade_status_count",
    )
    .agg(F.sum("enrollment_count_2").alias("total_enrollment_count_2"))
)
