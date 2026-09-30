from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_ids = Window.partitionBy("distinct_ids")
w_dept_credits_by_distinct_ids = Window.partitionBy("dept", "credits").orderBy("distinct_ids", "dept", "credits", "level_or_unknown")
w_dept_credits_by_distinct_ids_2 = Window.partitionBy("dept", "credits").orderBy("distinct_ids", "dept", "credits", "level_or_unknown").rowsBetween(-2, Window.currentRow)

base_courses = (
    df_courses
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.when(F.col("level").isin("intro", "advanced"), "group_a").when(F.col("level") == "core", "group_b").otherwise("other").alias("level_group"),
        F.concat_ws("-", F.col("level"), F.col("dept")).alias("level_dept_label"),
        F.coalesce(F.col("level"), F.lit("UNKNOWN")).alias("level_or_unknown"),
    )
)

rollup_base = (
    base_courses
    .filter(F.col("credits") >= 2)
    .groupBy("dept", "credits", "level_or_unknown")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("credits"), 2).alias("stddev_credits"),
        F.array_sort(F.collect_set("dept")).alias("dept_list"),
        F.avg("credits").alias("avg_credits_check"),
    )
    .filter(F.col("avg_credits_check") > 2)
    .drop("avg_credits_check")
)

with_windows = (
    rollup_base
    .select(
        "dept",
        "credits",
        "level_or_unknown",
        "distinct_ids",
        "stddev_credits",
        F.size(F.collect_set("level_or_unknown").over(w_distinct_ids)).alias("distinct_level_or_unknown_in_distinct_ids"),
        F.round(F.avg("credits").over(w_distinct_ids), 2).alias("distinct_ids_avg_credits"),
        F.lag("stddev_credits", 3, 0).over(w_dept_credits_by_distinct_ids).alias("prev3_stddev_credits"),
        F.max("distinct_ids").over(w_dept_credits_by_distinct_ids_2).alias("distinct_ids_max_3"),
    )
)

result = (
    with_windows
    .select(
        "dept",
        "credits",
        "distinct_ids",
        "distinct_level_or_unknown_in_distinct_ids",
        "prev3_stddev_credits",
        "distinct_ids_max_3",
    )
    .orderBy(F.col("prev3_stddev_credits").desc())
)
