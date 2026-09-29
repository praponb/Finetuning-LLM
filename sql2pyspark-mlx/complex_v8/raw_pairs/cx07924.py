from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_avg_credits_running = Window.orderBy(F.col("avg_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_group_dept = Window.partitionBy("level_group", "dept")

prepared_courses = (
    df_courses
    .filter((F.upper(F.trim(F.col("course_code"))).like("M%")) & (F.col("level").isNotNull()))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.when(F.col("level").isin("core", "advanced"), "group_a").when(F.col("level") == "intro", "group_b").otherwise("other").alias("level_group"),
    )
)

derived = (
    prepared_courses
    .select(
        "*",
        F.coalesce(F.col("level"), F.lit("UNKNOWN")).alias("level_or_unknown"),
        F.when(F.col("level").isin("advanced", "core"), "group_a").when(F.col("level") == "intro", "group_b").otherwise("other").alias("level_group_2"),
        F.when(F.col("level_group").isin("other", "group_a"), "group_a").when(F.col("level_group") == "group_b", "group_b").otherwise("other").alias("level_group_group"),
    )
)

level_group_2_stats = (
    derived
    .groupBy("level_group_2")
    .agg(F.avg("credits").alias("avg_credits"))
)

compared = (
    derived.alias("s")
    .join(level_group_2_stats.alias("g"), F.col("g.level_group_2") == F.col("s.level_group_2"), "inner")
    .filter(F.col("s.credits") > F.col("g.avg_credits"))
    .select(
        "s.*",
        "g.avg_credits",
        F.round(F.col("s.credits") - F.col("g.avg_credits"), 2).alias("credits_minus_avg"),
    )
)

sequenced = (
    compared
    .withColumn("last_known_credits_minus_avg", F.last("credits_minus_avg", ignorenulls=True).over(w_all_by_avg_credits_running))
    .withColumn("level_group_dept_min_credits_minus_avg", F.min("credits_minus_avg").over(w_level_group_dept))
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

summarized_rows = (
    sequenced.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
        F.min("score").alias("min_enrollment_score"),
    )
)

profiled = (
    summarized_rows.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.min_enrollment_score",
    )
)

result = (
    profiled
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        "level_or_unknown",
        "level_group_2",
        "level_group_group",
        "avg_credits",
        "credits_minus_avg",
        "level_group_dept_min_credits_minus_avg",
        "last_enrollment_enrolled_at",
        "first_enrollment_enrolled_at",
        "min_enrollment_score",
        F.lower(F.regexp_replace(F.trim(F.col("course_code")), " ", "_")).alias("course_code_slug"),
    )
    .orderBy(F.col("course_code_slug").asc(), "last_enrollment_enrolled_at")
)
