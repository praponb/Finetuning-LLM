from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_avg_credits_distance = Window.orderBy("avg_credits_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_credits_distance_credits = Window.partitionBy("credits_distance", "credits")

valid = (
    df_courses
    .filter((F.col("level") != "intro") & (F.col("credits") >= 2))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.abs(F.col("credits") - 2).alias("credits_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("dept")), " ", "_")).alias("dept_slug"),
    )
)

level_stats = (
    valid
    .groupBy("level")
    .agg(F.avg("credits_distance").alias("avg_credits_distance"))
)

outliers = (
    valid.alias("s")
    .join(level_stats.alias("g"), F.col("g.level") == F.col("s.level"), "inner")
    .filter(F.col("s.credits_distance") > F.col("g.avg_credits_distance"))
    .select(
        "s.*",
        "g.avg_credits_distance",
        F.round(F.col("s.credits_distance") - F.col("g.avg_credits_distance"), 2).alias("credits_distance_minus_avg"),
    )
)

metrics = (
    outliers
    .select(
        "id",
        "dept",
        "credits",
        "level",
        "credits_distance",
        "dept_slug",
        "avg_credits_distance",
        F.min("credits_distance").over(w_all_by_avg_credits_distance).alias("min_credits_distance_so_far"),
        F.count(F.lit(1)).over(w_credits_distance_credits).alias("rows_in_credits_distance_credits"),
    )
)

filtered = (
    metrics.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.student_id") <= 17), "left_semi")
)

result = (
    filtered
    .groupingSets(
        [["dept", "credits"], ["dept"], ["credits"], []],
        "dept", "credits",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count(F.when(F.col("level") == "advanced", 1)).alias("n_level_advanced"),
        F.count("*").alias("cnt"),
    )
    .select(
        F.coalesce(F.col("dept"), F.lit("ALL")).alias("dept"),
        "credits",
        "gid",
        "n_level_advanced",
        "cnt",
    )
)
