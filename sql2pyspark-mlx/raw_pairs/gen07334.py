from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("element_rounded").desc_nulls_first())
w3 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())
w4 = Window.partitionBy(F.month("effective_from"), "plan").orderBy(F.col("element_rounded").desc())
w_hist = Window.partitionBy("plan").orderBy(F.col("element_rounded").asc_nulls_last())
w6 = Window.partitionBy(F.date_trunc("week", "effective_from"), "plan")
w7 = Window.partitionBy("plan").orderBy(F.col("element_rounded_percent_rank_2").asc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("element"))
)

valid = (
    flattened
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-05-21"))) & F.col("element").between(21, 80))
    .select("*", F.round("element", 1).alias("element_rounded"))
)

with_windows = (
    valid
    .select(
        "id",
        "plan",
        "effective_from",
        "element_rounded",
        F.stddev_samp("element_rounded").over(w).alias("plan_stddev_samp_element_rounded"),
        F.cume_dist().over(w2).alias("element_rounded_cume_dist"),
        F.cume_dist().over(w3).alias("element_cume_dist"),
        F.percent_rank().over(w4).alias("element_rounded_percent_rank"),
        F.percent_rank().over(w_hist).alias("element_rounded_percent_rank_2"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.stddev_samp("element_cume_dist").over(w6).alias("effective_from_period_stddev_samp_element_cume_dist"),
        F.cume_dist().over(w7).alias("element_rounded_percent_rank_2_cume_dist"),
        F.stddev_samp("plan_stddev_samp_element_rounded").over(w).alias("plan_stddev_samp_plan_stddev_samp_element_rounded"),
    )
)

base = (
    metrics
    .filter(F.col("effective_from") > F.lit("2024-02-11").cast("date"))
    .select(
        "*",
        (F.col("element_cume_dist") / F.nullif("element_rounded_cume_dist", F.lit(0))).alias("element_cume_dist_per_element_rounded_cume_dist"),
        F.col("element_rounded_percent_rank_2").cast("int").alias("element_rounded_percent_rank_2_int"),
    )
)

summary = (
    base
    .groupBy("plan")
    .agg(
        F.round(F.avg("element_rounded_cume_dist"), 2).alias("avg_element_rounded_cume_dist"),
        F.max("plan_stddev_samp_plan_stddev_samp_element_rounded").alias("max_plan_stddev_samp_plan_stddev_samp_element_rounded"),
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
)

result = (
    summary
    .select(
        "plan",
        "avg_element_rounded_cume_dist",
        "max_plan_stddev_samp_plan_stddev_samp_element_rounded",
        "starter_events",
    )
)
