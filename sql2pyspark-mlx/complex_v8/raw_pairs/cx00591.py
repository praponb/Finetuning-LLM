from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_group_len_level_group = Window.partitionBy("level_group_len", "level_group").orderBy("median_credits", "level_group")
w_level_group_len_level_group_2 = Window.partitionBy("level_group_len", "level_group").orderBy("median_credits", "level_group").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_group_len_level_group_3 = Window.partitionBy("level_group_len", "level_group").orderBy("median_credits")

valid_courses = (
    df_courses
    .filter(F.col("dept").isNotNull())
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.when(F.col("level").isin("advanced", "core"), "group_a").when(F.col("level") == "intro", "group_b").otherwise("other").alias("level_group"),
    )
)

stats = (
    valid_courses
    .groupBy("level_group")
    .agg(
        F.sum("credits").alias("total_credits"),
        F.max("credits").alias("max_credits"),
        F.percentile_approx("credits", 0.5).alias("median_credits"),
        F.round(100 * F.count(F.when(F.col("level") == "advanced", 1)) / F.count("*"), 1).alias("pct_level_advanced"),
    )
    .filter(F.col("total_credits") > 2)
)

filtered = (
    stats
    .crossJoin(stats.agg(F.avg("pct_level_advanced").alias("overall_avg_pct_level_advanced")))
    .filter(F.col("pct_level_advanced") > F.col("overall_avg_pct_level_advanced"))
    .drop("overall_avg_pct_level_advanced")
)

scored = (
    filtered
    .filter(F.col("level_group") == "group_a")
    .select(
        "level_group",
        "median_credits",
        "pct_level_advanced",
        F.length(F.trim(F.col("level_group"))).alias("level_group_len"),
        F.when(F.col("level_group").isin("group_a", "other"), "group_a").when(F.col("level_group") == "group_b", "group_b").otherwise("other").alias("level_group_group"),
    )
)

level_group_group_stats = (
    scored
    .groupBy("level_group_group")
    .agg(F.max("pct_level_advanced").alias("max_pct_level_advanced"))
)

vs_group = (
    scored.alias("s")
    .join(level_group_group_stats.alias("g"), F.col("g.level_group_group") == F.col("s.level_group_group"), "inner")
    .filter(F.col("s.pct_level_advanced") == F.col("g.max_pct_level_advanced"))
    .select(
        "s.*",
        "g.max_pct_level_advanced",
        F.round(F.col("s.pct_level_advanced") - F.col("g.max_pct_level_advanced"), 2).alias("pct_level_advanced_minus_max"),
    )
)

metrics = (
    vs_group
    .select(
        "level_group",
        "median_credits",
        "pct_level_advanced",
        "level_group_group",
        F.rank().over(w_level_group_len_level_group).alias("median_credits_rank"),
        F.first("pct_level_advanced", ignorenulls=True).over(w_level_group_len_level_group_2).alias("first_known_pct_level_advanced"),
        F.dense_rank().over(w_level_group_len_level_group_3).alias("median_credits_dense_rank"),
        F.lead("level_group_len").over(w_level_group_len_level_group).alias("next_level_group_len"),
        F.round(F.cume_dist().over(w_level_group_len_level_group_3), 3).alias("median_credits_cume_dist"),
    )
)

enriched = (
    metrics
    .select(
        "level_group",
        "median_credits",
        "pct_level_advanced",
        "level_group_group",
        "median_credits_rank",
        "median_credits_dense_rank",
        "next_level_group_len",
        "median_credits_cume_dist",
        (F.col("pct_level_advanced") - F.col("median_credits_rank")).alias("pct_level_advanced_minus_median_credits_rank"),
    )
)

result = (
    enriched
    .select(
        "level_group",
        "median_credits",
        "next_level_group_len",
        "median_credits_rank",
        "median_credits_dense_rank",
    )
    .unpivot(
        ids=["level_group"],
        values=["median_credits", "next_level_group_len", "median_credits_rank", "median_credits_dense_rank"],
        variableColumnName="field",
        valueColumnName="reading",
    )
)
