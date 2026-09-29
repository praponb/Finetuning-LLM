from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())

lines = df_price_tiers.select("id", "plan", F.explode_outer("tier_prices").alias("element"))

metrics = (
    lines
    .filter(F.col("plan") == "enterprise")
    .select(
        "*",
        F.stddev_samp("element").over(w).alias("plan_stddev_samp_element"),
        F.stddev_samp("element").over(w).alias("plan_stddev_samp_element_2"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
        F.stddev_samp("element").over(w).alias("plan_stddev_samp_element_3"),
        F.count("element").over(w).alias("plan_count_element"),
    )
)

base = (
    metrics
    .filter(F.col("element_percent_rank") > 0.46)
    .withColumn("element_percent_rank_per_plan_stddev_samp_element_2", (F.col("element_percent_rank") / F.nullif("plan_stddev_samp_element_2", F.lit(0))))
)

filtered = (
    base
    .filter(F.col("element_percent_rank_per_plan_stddev_samp_element_2") >= 58)
    .withColumn("plan_stddev_samp_element_3_minus_element", (F.col("plan_stddev_samp_element_3") - F.col("element")))
)

filtered_price_tiers = (
    filtered
    .filter(F.col("element_percent_rank_per_plan_stddev_samp_element_2") > 67)
    .select(
        "*",
        (F.col("element") / F.nullif("plan_stddev_samp_element_3", F.lit(0))).alias("element_per_plan_stddev_samp_element_3"),
        F.round((F.col("element_percent_rank_per_plan_stddev_samp_element_2") * 0.85), 2).alias("element_percent_rank_per_plan_stddev_samp_element_2_adjusted"),
    )
)

cleaned = (
    filtered_price_tiers
    .filter(F.col("plan_stddev_samp_element_2") == 38)
    .withColumn("plan_stddev_samp_element_2_minus_plan_stddev_samp_element_3_minus_element", (F.col("plan_stddev_samp_element_2") - F.col("plan_stddev_samp_element_3_minus_element")))
)

valid = (
    cleaned
    .filter(F.col("element_percent_rank_per_plan_stddev_samp_element_2_adjusted") <= 69)
    .select(
        "*",
        F.least("element", "plan_stddev_samp_element_2").alias("least_element_plan_stddev_samp_element_2"),
        F.coalesce("plan_count_element", F.lit(0)).alias("plan_count_element_filled"),
    )
)

result = (
    valid
    .groupBy("plan")
    .agg(
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.round(F.avg("element_per_plan_stddev_samp_element_3"), 2).alias("avg_element_per_plan_stddev_samp_element_3"),
        F.max(F.when(F.col("plan") == "starter", F.col("plan_count_element"))).alias("starter_max_plan_count_element"),
        F.min("least_element_plan_stddev_samp_element_2").alias("min_least_element_plan_stddev_samp_element_2"),
        F.percentile_approx("element_percent_rank", [0.25, 0.5, 0.75]).alias("element_percent_rank_quartiles"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .orderBy(F.col("avg_element_per_plan_stddev_samp_element_3").asc_nulls_last())
)
