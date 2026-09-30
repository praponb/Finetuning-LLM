from pyspark.sql import functions as F

base_courses = (
    df_courses
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
    )
)

stats = (
    base_courses
    .groupBy("level", "dept", "credits")
    .agg(
        F.sum("credits").alias("total_credits"),
        F.round(F.stddev("credits"), 2).alias("stddev_credits"),
        F.array_sort(F.collect_set("level")).alias("level_list"),
        F.count("*").alias("row_count"),
        F.countDistinct("dept").alias("distinct_depts"),
        F.array_sort(F.collect_set("dept")).alias("dept_list"),
    )
)

labeled = (
    stats
    .filter(F.col("distinct_depts") <= 7)
    .select(
        "*",
        F.length(F.trim(F.col("level"))).alias("level_len"),
        (F.floor(F.col("row_count") / 2) * 2).alias("row_count_bucket"),
    )
)

tagged = labeled.withColumn("distinct_depts_filled", F.coalesce(F.col("distinct_depts"), F.lit(0)))

result = (
    tagged
    .select(
        "level",
        "credits",
        "stddev_credits",
        "level_list",
        "distinct_depts",
        "dept_list",
        "level_len",
        "row_count_bucket",
        "distinct_depts_filled",
    )
    .orderBy(F.col("credits").asc())
)
