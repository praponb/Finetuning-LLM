from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_credits = Window.partitionBy("credits")
w_credits_filled_by_least_avg = Window.partitionBy("credits_filled").orderBy("least_avg_credits_adj_avg_credits", "credits_filled", "credits")
w_credits_filled_distinct_leve = Window.partitionBy("credits_filled", "distinct_levels").orderBy(F.col("distinct_levels").desc(), "credits_filled", "credits")
w_credits_filled = Window.partitionBy("credits_filled")

scoped = (
    df_courses
    .filter((F.length(F.trim(F.col("course_code"))) >= 7) & (~F.col("dept").isin("Art", "Biz")))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.when(F.col("level").isin("core", "intro"), "group_a").when(F.col("level") == "advanced", "group_b").otherwise("other").alias("level_group"),
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
    )
)

with_windows = (
    scoped
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        "credits_filled",
        F.sum("credits").over(w_credits).alias("credits_sum_credits"),
        F.min("credits").over(w_credits).alias("credits_min_credits"),
    )
)

totals = (
    with_windows
    .filter(F.upper(F.trim(F.col("course_code"))).like("C%"))
    .groupBy("credits_filled", "credits")
    .agg(
        F.round(F.sum("credits_min_credits") / F.nullif(F.sum("credits_filled"), F.lit(0)), 3).alias("credits_min_credits_to_credits_filled_ratio"),
        F.round(F.avg("credits"), 2).alias("avg_credits"),
        F.sum("credits_sum_credits").alias("total_credits_sum_credits"),
        F.countDistinct("level").alias("distinct_levels"),
        F.round(F.sum("credits_min_credits") / F.nullif(F.sum("credits_sum_credits"), F.lit(0)), 3).alias("credits_min_credits_to_credits_sum_credits_ratio"),
        F.max("credits_filled").alias("max_credits_filled_check"),
    )
    .filter(F.col("max_credits_filled_check") > 2)
    .drop("max_credits_filled_check")
)

enriched = (
    totals
    .filter(F.col("distinct_levels") > 5)
    .select(
        "credits_filled",
        "credits",
        "credits_min_credits_to_credits_filled_ratio",
        "avg_credits",
        "distinct_levels",
        "credits_min_credits_to_credits_sum_credits_ratio",
        F.round(F.col("avg_credits") * 1.1, 2).alias("avg_credits_adj"),
    )
)

annotated = (
    enriched
    .filter(F.col("credits") <= 4)
    .withColumn("least_avg_credits_adj_avg_credits", F.least(F.col("avg_credits_adj"), F.col("avg_credits")))
)

with_windows_2 = (
    annotated
    .select(
        "*",
        F.lead("distinct_levels", 2).over(w_credits_filled_by_least_avg).alias("next2_distinct_levels"),
        F.lead("avg_credits_adj").over(w_credits_filled_by_least_avg).alias("next_avg_credits_adj"),
        (F.col("avg_credits") - F.lag("avg_credits").over(w_credits_filled_distinct_leve)).alias("avg_credits_change"),
        F.count(F.lit(1)).over(w_credits_filled).alias("rows_in_credits_filled"),
        F.round(F.col("avg_credits") / F.nullif(F.sum("avg_credits").over(w_credits_filled), F.lit(0)), 4).alias("avg_credits_share_of_credits_filled"),
    )
)

result = (
    with_windows_2
    .select(
        "credits_filled",
        "credits",
        "avg_credits_change",
        "credits_min_credits_to_credits_filled_ratio",
        "avg_credits",
    )
    .unpivot(
        ids=["credits_filled", "credits"],
        values=["avg_credits_change", "credits_min_credits_to_credits_filled_ratio", "avg_credits"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
