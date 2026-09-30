from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").desc())
w2 = Window.partitionBy("plan")

lines = df_price_tiers.select("id", "plan", F.explode_outer("tier_prices").alias("element"))

kept_keys = (
    lines
    .select("id", "plan")
    .subtract(
        df_price_tiers
        .filter(F.col("currency").isin(["THB", "EUR"]))
        .select("id", "plan")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .filter(F.col("plan") != "enterprise")
    .select(
        "*",
        F.rank().over(w).alias("element_rank"),
        F.rank().over(w).alias("element_rank_2"),
        F.stddev_samp("element").over(w2).alias("plan_stddev_samp_element"),
        F.count("element").over(w2).alias("plan_count_element"),
    )
)

valid = (
    enriched
    .filter(F.col("plan_stddev_samp_element").between(60, 65))
    .select(
        "id",
        "plan",
        "element_rank_2",
        "plan_count_element",
        (F.col("element_rank_2") - F.col("plan_stddev_samp_element")).alias("element_rank_2_minus_plan_stddev_samp_element"),
    )
)

kept_keys_2 = (
    valid
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.col("currency") != "EUR")
        .select("id")
    )
)

kept_rows_price_tiers = (
    valid
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows_price_tiers
    .groupBy(
        F.when(F.col("element_rank_2_minus_plan_stddev_samp_element") >= 73, "LARGE").otherwise("SMALL").alias("element_rank_2_minus_plan_stddev_samp_element_tier"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "pro", F.col("element_rank_2_minus_plan_stddev_samp_element")).otherwise(0)).alias("pro_element_rank_2_minus_plan_stddev_samp_element"),
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
        F.avg("element_rank_2").alias("avg_element_rank_2"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
)
