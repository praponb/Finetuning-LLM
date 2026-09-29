from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("level", "level_or_unknown").orderBy("course_code_len", "id")
w_credits_by_min_enrollment_sc = Window.partitionBy("credits").orderBy(F.col("min_enrollment_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrollment_count = Window.partitionBy("enrollment_count")

prepared_courses = df_courses.filter(F.col("credits") >= 2)

enriched = (
    prepared_courses
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        F.coalesce(F.col("level"), F.lit("UNKNOWN")).alias("level_or_unknown"),
        F.length(F.trim(F.col("course_code"))).alias("course_code_len"),
    )
)

latest_per_key = (
    enriched
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

qualified = (
    latest_per_key.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left_semi")
    .filter(F.col("level_or_unknown").isin("UNKNOWN", "core", "intro"))
)

labeled = (
    qualified
    .select(
        "*",
        (F.col("course_code_len") - F.col("credits")).alias("course_code_len_minus_credits"),
        F.when(F.col("level_or_unknown").isin("intro", "core"), "group_a").when(F.col("level_or_unknown") == "UNKNOWN", "group_b").otherwise("other").alias("level_or_unknown_group"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.min("score").alias("min_enrollment_score"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

with_stats = (
    labeled.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.min_enrollment_score",
        "e.first_enrollment_enrolled_at",
    )
)

sequenced = (
    with_stats
    .select(
        "*",
        F.nth_value("first_enrollment_enrolled_at", 2).over(w_credits_by_min_enrollment_sc).alias("second_first_enrollment_enrolled_at"),
        F.size(F.collect_set("level_or_unknown").over(w_enrollment_count)).alias("distinct_level_or_unknown_in_enrollment_count"),
        F.first("enrollment_count").over(w_credits_by_min_enrollment_sc).alias("first_enrollment_count"),
        F.size(F.collect_set("level").over(w_enrollment_count)).alias("distinct_level_in_enrollment_count"),
        F.max("min_enrollment_score").over(w_enrollment_count).alias("enrollment_count_max_min_enrollment_score"),
        F.min("credits").over(w_enrollment_count).alias("enrollment_count_min_credits"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["distinct_level_in_enrollment_count", "course_code_len", "distinct_level_or_unknown_in_enrollment_count"], ["distinct_level_in_enrollment_count"]],
        "distinct_level_in_enrollment_count", "course_code_len", "distinct_level_or_unknown_in_enrollment_count",
    )
    .agg(
        F.grouping("distinct_level_in_enrollment_count").alias("is_total_distinct_level_in_enrollment_count"),
        F.round(F.avg("course_code_len"), 2).alias("avg_course_code_len"),
        F.array_sort(F.collect_set("course_code")).alias("course_code_list"),
        F.min("enrollment_count").alias("min_enrollment_count"),
    )
    .orderBy(
        F.col("distinct_level_in_enrollment_count").asc_nulls_last(),
        F.col("course_code_len").asc_nulls_last(),
        F.col("distinct_level_or_unknown_in_enrollment_count").asc_nulls_last(),
    )
)
