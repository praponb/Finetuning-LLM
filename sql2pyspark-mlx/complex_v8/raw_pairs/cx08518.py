from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_dept_group = Window.partitionBy("level", "dept_group")
w_all_by_credits_moving_2 = Window.orderBy("credits", "id").rowsBetween(-2, Window.currentRow)
w_all_by_credits_running = Window.orderBy(F.col("credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_credits = Window.orderBy("credits", "id")
w_all_by_credits_ties = Window.orderBy(F.col("credits").desc())
w_min_next_credits_by_min_next = Window.partitionBy("min_next_credits").orderBy(F.col("min_next_credits").desc())
w_distinct_dept_groups_distinc = Window.partitionBy("distinct_dept_groups", "distinct_depts").orderBy(F.col("total_credits_tercile").desc())
w_min_next_credits_by_min_next_2 = Window.partitionBy("min_next_credits").orderBy(F.col("min_next_credits").desc(), "level", "next_credits", "credits").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_courses
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.concat(F.col("course_code"), F.lit("#"), F.col("id").cast("string")).alias("course_code_ref"),
        F.when(F.col("dept").isin("Math", "CS"), "group_a").when(F.col("dept") == "Art", "group_b").otherwise("other").alias("dept_group"),
    )
)

sequenced = (
    valid
    .select(
        "*",
        F.sum("credits").over(w_level_dept_group).alias("level_dept_group_sum_credits"),
        F.round(F.avg("credits").over(w_all_by_credits_moving_2), 2).alias("credits_avg_3"),
        F.round(F.col("credits") - F.avg("credits").over(w_level_dept_group), 2).alias("credits_vs_level_dept_group_avg"),
        F.max("credits").over(w_level_dept_group).alias("level_dept_group_max_credits"),
        F.first("credits", ignorenulls=True).over(w_all_by_credits_running).alias("first_known_credits"),
        F.ntile(3).over(w_all_by_credits).alias("credits_tercile"),
        F.lead("credits").over(w_all_by_credits).alias("next_credits"),
        F.round(F.percent_rank().over(w_all_by_credits_ties), 3).alias("credits_percent_rank"),
        F.round(F.avg("credits").over(w_level_dept_group), 2).alias("level_dept_group_avg_credits"),
        F.count(F.lit(1)).over(w_level_dept_group).alias("rows_in_level_dept_group"),
        F.size(F.collect_set("dept").over(w_level_dept_group)).alias("distinct_dept_in_level_dept_group"),
        F.lag("credits", 3).over(w_all_by_credits).alias("prev3_credits"),
    )
)

rollup_base = (
    sequenced
    .filter(F.col("prev3_credits") < 3)
    .groupBy("level", "next_credits", "credits")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("dept").alias("distinct_depts"),
        F.min("next_credits").alias("min_next_credits"),
        F.round(F.avg("credits_percent_rank"), 2).alias("avg_credits_percent_rank"),
        F.sum("credits_tercile").alias("total_credits_tercile"),
        F.countDistinct("dept_group").alias("distinct_dept_groups"),
    )
)

windowed = (
    rollup_base
    .select(
        "*",
        F.dense_rank().over(w_min_next_credits_by_min_next).alias("min_next_credits_dense_rank"),
        F.rank().over(w_distinct_dept_groups_distinc).alias("total_credits_tercile_rank"),
        F.nth_value("min_next_credits", 2).over(w_min_next_credits_by_min_next_2).alias("second_min_next_credits"),
    )
)

result = (
    windowed
    .select(
        "level",
        "next_credits",
        "row_count",
        "distinct_depts",
        "min_next_credits",
        "avg_credits_percent_rank",
        "total_credits_tercile",
        "distinct_dept_groups",
        "total_credits_tercile_rank",
        "second_min_next_credits",
    )
    .orderBy(F.col("total_credits_tercile").desc(), "total_credits_tercile_rank")
)
