from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_by_credits_running = Window.partitionBy("level").orderBy(F.col("credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_credits_dept_by_credits_prio = Window.partitionBy("credits", "dept").orderBy("credits", "id").rowsBetween(-1, -1)
w_credits_dept_by_credits_full = Window.partitionBy("credits", "dept").orderBy(F.col("credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_credits_level = Window.partitionBy("credits", "level")
w_level_by_credits_full = Window.partitionBy("level").orderBy(F.col("credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_credits_dept_by_credits = Window.partitionBy("credits", "dept").orderBy("credits", "id")
w_credits_dept_by_credits_prio_2 = Window.partitionBy("credits", "dept").orderBy(F.col("credits").desc(), "id").rowsBetween(-3, -1)

cleaned = (
    df_courses
    .filter(F.col("dept") == "Bio")
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.initcap(F.trim(F.col("dept"))).alias("dept_clean"),
    )
)

with_trends = (
    cleaned
    .select(
        "id",
        "dept",
        "credits",
        "level",
        "dept_clean",
        F.first("credits", ignorenulls=True).over(w_level_by_credits_running).alias("first_known_credits"),
        F.round(F.avg("credits").over(w_credits_dept_by_credits_prio), 2).alias("credits_avg_1_prior"),
        F.last("credits").over(w_credits_dept_by_credits_full).alias("last_credits"),
        F.round(F.col("credits") / F.nullif(F.sum("credits").over(w_credits_level), F.lit(0)), 4).alias("credits_share_of_credits_level"),
        F.max("credits").over(w_credits_level).alias("credits_level_max_credits"),
        F.nth_value("credits", 2).over(w_level_by_credits_full).alias("second_credits"),
        F.lead("credits").over(w_credits_dept_by_credits).alias("next_credits"),
        F.round(F.col("credits") - F.avg("credits").over(w_credits_level), 2).alias("credits_vs_credits_level_avg"),
        F.round(F.avg("credits").over(w_credits_level), 2).alias("credits_level_avg_credits"),
        F.sum("credits").over(w_credits_dept_by_credits_prio_2).alias("credits_sum_3_prior"),
    )
)

enriched = (
    with_trends
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("dept_clean")), " ", "_")).alias("dept_clean_slug"),
        F.concat(F.col("dept"), F.lit("#"), F.col("first_known_credits").cast("string")).alias("dept_ref"),
        F.round(F.col("credits_sum_3_prior") / F.nullif(F.col("first_known_credits"), F.lit(0)), 3).alias("credits_sum_3_prior_per_first_known_credits"),
    )
)

result = (
    enriched
    .filter(F.col("credits_vs_credits_level_avg") >= -2.0)
    .select(
        "id",
        "dept",
        "credits",
        "level",
        "dept_clean",
        "first_known_credits",
        "credits_avg_1_prior",
        "last_credits",
        "credits_share_of_credits_level",
        "credits_level_max_credits",
        "second_credits",
        "next_credits",
        "credits_vs_credits_level_avg",
        "credits_level_avg_credits",
        "credits_sum_3_prior",
        "dept_ref",
        "credits_sum_3_prior_per_first_known_credits",
        F.substring(F.trim(F.col("dept_clean_slug")), 1, 2).alias("dept_clean_slug_prefix"),
        F.least(F.col("credits_sum_3_prior"), F.col("credits")).alias("least_credits_sum_3_prior_credits"),
    )
    .orderBy(F.col("credits_avg_1_prior").desc())
)
