from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_by_credits_running = Window.partitionBy("level").orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_or_unknown_level = Window.partitionBy("level_or_unknown", "level")
w_all_by_credits = Window.orderBy("credits", "id")
w_dept = Window.partitionBy("dept")
w_level_by_credits_full = Window.partitionBy("level").orderBy(F.col("credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_credits = Window.partitionBy("level", "credits")
w_level_by_credits_ties = Window.partitionBy("level").orderBy("credits")
w_level_by_credits = Window.partitionBy("level").orderBy(F.col("credits").desc(), "id")
w_credits_level_by_credits_dec = Window.partitionBy("credits", "level").orderBy(F.col("credits_decile_per_first_credits").asc_nulls_last(), "id")
w_level_or_unknown_by_running = Window.partitionBy("level_or_unknown").orderBy("running_credits", "id")
w_credits_rank_level_or_unknow = Window.partitionBy("credits_rank", "level_or_unknown")
w_distinct_dept_in_level_credi = Window.partitionBy("distinct_dept_in_level_credits", "credits_decile").orderBy(F.col("credits_vs_dept_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_dept_in_level_credi_2 = Window.partitionBy("distinct_dept_in_level_credits", "credits_decile").orderBy(F.col("credits_vs_dept_avg").desc(), "id").rowsBetween(-2, -1)

scoped = (
    df_courses
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("level"), F.lit("UNKNOWN")).alias("level_or_unknown"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.sum("credits").over(w_level_by_credits_running).alias("running_credits"),
        F.size(F.collect_set("dept").over(w_level_or_unknown_level)).alias("distinct_dept_in_level_or_unknown_level"),
        F.ntile(10).over(w_all_by_credits).alias("credits_decile"),
        F.round(F.col("credits") - F.avg("credits").over(w_dept), 2).alias("credits_vs_dept_avg"),
        F.first("credits").over(w_level_by_credits_full).alias("first_credits"),
        F.size(F.collect_set("dept").over(w_level_credits)).alias("distinct_dept_in_level_credits"),
        F.rank().over(w_level_by_credits_ties).alias("credits_rank"),
        F.lead("credits", 3).over(w_level_by_credits).alias("next3_credits"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        "level_or_unknown",
        "running_credits",
        "credits_decile",
        "credits_vs_dept_avg",
        "distinct_dept_in_level_credits",
        "credits_rank",
        "next3_credits",
        F.round(F.col("credits_decile") / F.nullif(F.col("first_credits"), F.lit(0)), 3).alias("credits_decile_per_first_credits"),
        F.upper(F.trim(F.col("level_or_unknown"))).alias("level_or_unknown_clean"),
        F.concat_ws(": ", F.col("level_or_unknown"), F.col("dept")).alias("level_or_unknown_dept_label"),
    )
)

ranked = (
    scored
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        "credits_vs_dept_avg",
        "distinct_dept_in_level_credits",
        "next3_credits",
        "credits_decile_per_first_credits",
        "level_or_unknown_clean",
        "level_or_unknown_dept_label",
        F.ntile(5).over(w_credits_level_by_credits_dec).alias("credits_decile_per_first_credits_quintile"),
        F.round(F.cume_dist().over(w_level_or_unknown_by_running), 3).alias("running_credits_cume_dist"),
        F.size(F.collect_set("level").over(w_credits_rank_level_or_unknow)).alias("distinct_level_in_credits_rank_level_or_unknown"),
        F.min("credits").over(w_distinct_dept_in_level_credi).alias("min_credits_so_far"),
        F.round(F.avg("next3_credits").over(w_distinct_dept_in_level_credi_2), 2).alias("next3_credits_avg_2_prior"),
    )
)

result = (
    ranked
    .select(
        "id",
        "course_code",
        "credits",
        "next3_credits",
        "level_or_unknown_clean",
        "level_or_unknown_dept_label",
        "credits_decile_per_first_credits_quintile",
        "running_credits_cume_dist",
        "distinct_level_in_credits_rank_level_or_unknown",
        "min_credits_so_far",
        F.abs(F.col("credits") - 3).alias("credits_distance"),
        F.round(F.col("credits_vs_dept_avg") * 1.5, 2).alias("credits_vs_dept_avg_adj"),
    )
)
