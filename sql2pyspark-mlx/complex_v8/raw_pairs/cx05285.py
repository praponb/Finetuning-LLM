from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_by_credits = Window.partitionBy("level").orderBy("credits", "id")
w_credits_by_credits = Window.partitionBy("credits").orderBy(F.col("credits").desc(), "id")
w_credits = Window.partitionBy("credits")
w_level_by_credits_full = Window.partitionBy("level").orderBy(F.col("credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_credits_level = Window.partitionBy("credits", "level")
w_level_credits_by_credits = Window.partitionBy("level", "credits").orderBy("credits", "id")
w_credits_by_credits_ties = Window.partitionBy("credits").orderBy(F.col("credits").desc())
w_level = Window.partitionBy("level")
w_level_credits_by_credits_pri = Window.partitionBy("level", "credits").orderBy(F.col("credits").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_distinct_level_in_credits = Window.partitionBy("distinct_level_in_credits")
w_credits_rank_by_credits_avg = Window.partitionBy("credits_rank").orderBy(F.col("credits_avg_1_prior").desc(), "id").rowsBetween(-6, -1)
w_credits_rank_credits_quartil = Window.partitionBy("credits_rank", "credits_quartile").orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_courses = (
    df_courses
    .filter((F.upper(F.trim(F.col("course_code"))).like("A%")) & (F.col("dept") != "Bio"))
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        F.initcap(F.trim(F.col("dept"))).alias("dept_clean"),
    )
)

ranked = (
    filtered_courses
    .select(
        "*",
        F.ntile(4).over(w_level_by_credits).alias("credits_quartile"),
        F.lead("credits").over(w_credits_by_credits).alias("next_credits"),
        F.size(F.collect_set("level").over(w_credits)).alias("distinct_level_in_credits"),
        F.nth_value("credits", 2).over(w_level_by_credits_full).alias("second_credits"),
        F.min("credits").over(w_credits).alias("credits_min_credits"),
        F.rank().over(w_level_credits_by_credits).alias("credits_rank"),
        (F.col("credits") - F.lag("credits").over(w_level_by_credits)).alias("credits_change"),
        F.round(F.percent_rank().over(w_credits_by_credits_ties), 3).alias("credits_percent_rank"),
        F.round(F.avg("credits").over(w_level_credits_by_credits_pri), 2).alias("credits_avg_1_prior"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "course_code",
        "credits",
        "dept_clean",
        "credits_quartile",
        "next_credits",
        "credits_min_credits",
        "credits_rank",
        "credits_change",
        "credits_percent_rank",
        "credits_avg_1_prior",
        F.count(F.lit(1)).over(w_distinct_level_in_credits).alias("rows_in_distinct_level_in_credits"),
        F.max("second_credits").over(w_credits_rank_by_credits_avg).alias("second_credits_max_6_prior"),
        F.last("credits_quartile", ignorenulls=True).over(w_credits_rank_credits_quartil).alias("last_known_credits_quartile"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "course_code",
        "credits",
        "dept_clean",
        "credits_quartile",
        "credits_min_credits",
        "credits_change",
        "credits_percent_rank",
        "credits_avg_1_prior",
        F.lower(F.regexp_replace(F.trim(F.col("dept_clean")), " ", "_")).alias("dept_clean_slug"),
    )
    .orderBy(F.col("credits_percent_rank").asc_nulls_last())
)
