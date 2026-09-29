from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("element_cume_dist").desc_nulls_last())
w3 = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("element"),
    )
)

enriched = lines.withColumn("element_cume_dist", F.cume_dist().over(w))

with_windows = (
    enriched
    .filter(F.col("plan") == "starter")
    .select(
        "*",
        F.percent_rank().over(w2).alias("element_cume_dist_percent_rank"),
        F.avg("element_cume_dist").over(w3).alias("plan_avg_element_cume_dist"),
    )
)

filtered = (
    with_windows
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-04-09")))
    .select(
        "id",
        "plan",
        "effective_from",
        "element",
        "element_cume_dist_percent_rank",
        "plan_avg_element_cume_dist",
        (F.col("plan_avg_element_cume_dist") - F.col("element")).alias("plan_avg_element_cume_dist_minus_element"),
    )
)

remaining_keys = (
    filtered
    .select("id", "plan")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from") > "2025-01-07")
        .select("id", "plan")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .cube(
        "plan",
        F.when(F.col("plan_avg_element_cume_dist_minus_element") >= 66, "PREMIUM").otherwise("STANDARD").alias("plan_avg_element_cume_dist_minus_element_tier"),
    )
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.round(F.avg("plan_avg_element_cume_dist_minus_element"), 2).alias("avg_plan_avg_element_cume_dist_minus_element"),
        F.round(F.avg("plan_avg_element_cume_dist_minus_element"), 2).alias("avg_plan_avg_element_cume_dist_minus_element_2"),
        F.sum(F.when(F.col("plan") == "basic", F.col("element_cume_dist_percent_rank")).otherwise(0)).alias("basic_element_cume_dist_percent_rank"),
        F.percentile_approx("element_cume_dist_percent_rank", 0.95).alias("element_cume_dist_percent_rank_p95"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
)
