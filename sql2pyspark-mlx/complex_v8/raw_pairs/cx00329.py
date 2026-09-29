from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_min_credits_by_distinct_ids = Window.partitionBy("min_credits").orderBy("distinct_ids", "level", "credits").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_by_distinct_ids_moving = Window.partitionBy("level").orderBy("distinct_ids", "level", "credits").rowsBetween(-1, Window.currentRow)
w_credits_min_credits = Window.partitionBy("credits", "min_credits")
w_min_credits_level_by_min_cre = Window.partitionBy("min_credits", "level").orderBy("min_credits", "level", "credits")
w_min_credits_level_by_min_cre_2 = Window.partitionBy("min_credits", "level").orderBy("min_credits", "level", "credits").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_min_credits_level_by_min_cre_3 = Window.partitionBy("min_credits", "level").orderBy("min_credits")
w_min_credits_level_by_min_cre_4 = Window.partitionBy("min_credits", "level").orderBy("min_credits", "level", "credits").rowsBetween(-6, -1)
w_distinct_ids = Window.partitionBy("distinct_ids")
w_all_by_second_credits_prior = Window.orderBy("second_credits", "level", "credits").rowsBetween(-1, -1)
w_all_by_second_credits_full = Window.orderBy("second_credits", "level", "credits").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_courses = (
    df_courses
    .filter(F.col("dept").isNotNull())
    .select(
        "id",
        "course_code",
        "credits",
        "level",
    )
)

summary = (
    prepared_courses
    .filter(F.col("level").isNotNull())
    .groupBy("level", "credits")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("credits").alias("min_credits"),
        F.sum(F.when(F.col("level").isin("core", "advanced"), 1).otherwise(0)).alias("n_level_in_core_advanced"),
    )
)

windowed = (
    summary
    .select(
        "*",
        F.min("credits").over(w_min_credits_by_distinct_ids).alias("min_credits_so_far"),
        F.round(F.avg("credits").over(w_level_by_distinct_ids_moving), 2).alias("credits_avg_2"),
    )
)

without_matches = (
    windowed
    .crossJoin(windowed.agg(F.avg("distinct_ids").alias("overall_avg_distinct_ids")))
    .filter(F.col("distinct_ids") > F.col("overall_avg_distinct_ids"))
    .drop("overall_avg_distinct_ids")
)

metrics = (
    without_matches
    .select(
        "*",
        F.round(F.col("distinct_ids") - F.avg("distinct_ids").over(w_credits_min_credits), 2).alias("distinct_ids_vs_credits_min_credits_avg"),
        F.lead("n_level_in_core_advanced", 2).over(w_min_credits_level_by_min_cre).alias("next2_n_level_in_core_advanced"),
        F.nth_value("credits", 2).over(w_min_credits_level_by_min_cre_2).alias("second_credits"),
        F.round(F.cume_dist().over(w_min_credits_level_by_min_cre_3), 3).alias("min_credits_cume_dist"),
        F.max("n_level_in_core_advanced").over(w_min_credits_level_by_min_cre_4).alias("n_level_in_core_advanced_max_6_prior"),
        F.min("min_credits").over(w_credits_min_credits).alias("credits_min_credits_min_min_credits"),
    )
)

tagged = (
    metrics
    .select(
        "level",
        "credits",
        "distinct_ids",
        "min_credits",
        "n_level_in_core_advanced",
        "min_credits_so_far",
        "distinct_ids_vs_credits_min_credits_avg",
        "next2_n_level_in_core_advanced",
        "second_credits",
        "min_credits_cume_dist",
        "n_level_in_core_advanced_max_6_prior",
        "credits_min_credits_min_min_credits",
        F.when(F.col("distinct_ids") >= 9, "A").when(F.col("distinct_ids") >= 4, "B").otherwise("C").alias("distinct_ids_tier"),
    )
)

sequenced = (
    tagged
    .select(
        "level",
        "credits",
        "distinct_ids",
        "min_credits",
        "min_credits_so_far",
        "distinct_ids_vs_credits_min_credits_avg",
        "next2_n_level_in_core_advanced",
        "second_credits",
        "credits_min_credits_min_min_credits",
        "distinct_ids_tier",
        F.count(F.lit(1)).over(w_distinct_ids).alias("rows_in_distinct_ids"),
        F.round(F.avg("credits").over(w_all_by_second_credits_prior), 2).alias("credits_avg_1_prior"),
        F.nth_value("n_level_in_core_advanced", 2).over(w_all_by_second_credits_full).alias("second_n_level_in_core_advanced"),
        F.min("credits").over(w_distinct_ids).alias("distinct_ids_min_credits"),
    )
)

result = (
    sequenced
    .select(
        "level",
        "credits",
        "min_credits",
        "min_credits_so_far",
        "next2_n_level_in_core_advanced",
        "credits_min_credits_min_min_credits",
        "distinct_ids_tier",
        "credits_avg_1_prior",
        "distinct_ids_min_credits",
    )
    .orderBy(F.col("min_credits").desc_nulls_first())
)
