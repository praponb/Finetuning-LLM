from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_dept_group = Window.partitionBy("level", "dept_group")

filtered = (
    df_courses
    .filter(F.lower(F.col("course_code")).like("%01%"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.concat_ws("-", F.col("course_code"), F.col("dept")).alias("course_code_dept_label"),
        F.length(F.trim(F.col("course_code"))).alias("course_code_len"),
        F.when(F.col("dept").isin("Bio", "Biz"), "group_a").when(F.col("dept") == "CS", "group_b").otherwise("other").alias("dept_group"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

profiled = (
    filtered.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

with_trends = (
    profiled
    .select(
        "*",
        F.round(F.col("min_enrollment_score") / F.nullif(F.sum("min_enrollment_score").over(w_level_dept_group), F.lit(0)), 4).alias("min_enrollment_score_share_of_level_dept_group"),
        F.count(F.lit(1)).over(w_level_dept_group).alias("rows_in_level_dept_group"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.sum("score").alias("total_enrollment_score"),
        F.min("score").alias("min_enrollment_score_2"),
    )
)

with_stats = (
    with_trends.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select("s.*", "e.total_enrollment_score", "e.min_enrollment_score_2")
)

qualified = (
    with_stats.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left_semi")
    .filter(F.col("min_enrollment_score_share_of_level_dept_group") > 0.4)
)

stats = (
    qualified
    .groupBy("credits")
    .agg(
        F.count(F.when(F.col("dept") == "Bio", 1)).alias("n_dept_bio"),
        F.sum(F.when(F.col("level") != "intro", 1).otherwise(0)).alias("n_level_intro"),
        F.max("rows_in_level_dept_group").alias("max_rows_in_level_dept_group"),
        F.count("*").alias("n_rows"),
        F.sum("rows_in_level_dept_group").alias("total_rows_in_level_dept_group"),
        F.percentile_approx("course_code_len", 0.5).alias("median_course_code_len"),
    )
)

screened = (
    stats
    .crossJoin(stats.agg(F.max("median_course_code_len").alias("overall_max_median_course_code_len")))
    .filter(F.col("median_course_code_len") < F.col("overall_max_median_course_code_len"))
    .drop("overall_max_median_course_code_len")
)

result = (
    screened
    .select(
        "credits",
        "n_level_intro",
        "max_rows_in_level_dept_group",
        "n_rows",
        "total_rows_in_level_dept_group",
        "median_course_code_len",
        F.least(F.col("n_dept_bio"), F.col("n_rows")).alias("least_n_dept_bio_n_rows"),
        F.coalesce(F.col("n_dept_bio"), F.lit(0)).alias("n_dept_bio_filled"),
    )
    .orderBy(F.col("total_rows_in_level_dept_group").asc())
)
