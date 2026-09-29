from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dept_credits = Window.partitionBy("dept", "credits")
w_credits_dept_by_credits = Window.partitionBy("credits", "dept").orderBy(F.col("credits").desc(), "id")
w_level = Window.partitionBy("level")
w_level_by_credits = Window.partitionBy("level").orderBy("credits", "id")
w_level_by_credits_full = Window.partitionBy("level").orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_credits_prior_6 = Window.orderBy("credits", "id").rowsBetween(-6, -1)
w_dept_level_by_credits = Window.partitionBy("dept", "level").orderBy("credits", "id")
w_level_by_credits_running = Window.partitionBy("level").orderBy(F.col("credits").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = df_courses.filter(F.col("level") == "intro")

ranked = (
    base
    .select(
        "*",
        F.round(F.avg("credits").over(w_dept_credits), 2).alias("dept_credits_avg_credits"),
        F.ntile(4).over(w_credits_dept_by_credits).alias("credits_quartile"),
        F.size(F.collect_set("dept").over(w_level)).alias("distinct_dept_in_level"),
        F.rank().over(w_credits_dept_by_credits).alias("credits_rank"),
        F.lead("credits", 2).over(w_level_by_credits).alias("next2_credits"),
        F.size(F.collect_set("level").over(w_dept_credits)).alias("distinct_level_in_dept_credits"),
        F.sum("credits").over(w_level).alias("level_sum_credits"),
        F.nth_value("credits", 2).over(w_level_by_credits_full).alias("second_credits"),
        F.lead("credits").over(w_level_by_credits).alias("next_credits"),
        F.max("credits").over(w_all_by_credits_prior_6).alias("credits_max_6_prior"),
        F.round(F.cume_dist().over(w_dept_level_by_credits), 3).alias("credits_cume_dist"),
        F.ntile(3).over(w_level_by_credits).alias("credits_tercile"),
        F.first("credits", ignorenulls=True).over(w_level_by_credits_running).alias("first_known_credits"),
        F.min("credits").over(w_level).alias("level_min_credits"),
    )
)

result = (
    ranked
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        "next2_credits",
        "distinct_level_in_dept_credits",
        "second_credits",
        "next_credits",
        "credits_cume_dist",
        "credits_tercile",
        F.initcap(F.trim(F.col("course_code"))).alias("course_code_clean"),
        F.coalesce(F.col("dept"), F.lit("UNKNOWN")).alias("dept_or_unknown"),
    )
    .orderBy("credits", F.col("level").asc_nulls_last())
)
