from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("is_basic_2").desc())
w_recent = Window.partitionBy("plan").orderBy(F.col("is_basic_2_dense_rank").desc_nulls_first())

flattened = df_price_tiers.select("id", "plan", F.explode("discounts").alias("tier_value"))

filtered = (
    flattened
    .filter(F.col("plan").isin(["enterprise", "basic", "starter"]))
    .withColumn("is_basic", F.when(F.col("plan") == "basic", 1).otherwise(0))
    .withColumn("is_basic_2", F.when(F.col("plan") == "basic", 1).otherwise(0))
)

matched_keys = (
    filtered
    .select("plan")
    .exceptAll(
        df_price_tiers
        .filter(F.col("plan") == "starter")
        .select("plan")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "plan",
        "is_basic_2",
        F.avg("tier_value").over(w_running).alias("plan_avg_tier_value"),
        F.dense_rank().over(w2).alias("is_basic_2_dense_rank"),
        F.stddev_samp("is_basic").over(w_running).alias("plan_stddev_samp_is_basic"),
    )
)

base = (
    metrics
    .filter(F.col("is_basic_2_dense_rank") < 4)
    .select("*", F.ceil("plan_avg_tier_value").alias("plan_avg_tier_value_whole"))
)

scored = (
    base
    .select(
        "plan",
        "is_basic_2",
        "plan_avg_tier_value",
        "is_basic_2_dense_rank",
        "plan_avg_tier_value_whole",
        F.percent_rank().over(w2).alias("is_basic_2_percent_rank"),
        F.percent_rank().over(w_recent).alias("is_basic_2_dense_rank_percent_rank"),
    )
)

result = (
    scored
    .withColumn("is_basic_2_dense_rank_tier", F.when(F.col("is_basic_2_dense_rank") >= 9, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["is_basic_2_dense_rank_tier", "plan"], ["is_basic_2_dense_rank_tier"], []],
        "is_basic_2_dense_rank_tier",
        "plan",
    )
    .agg(
        F.round(F.avg("is_basic_2"), 2).alias("avg_is_basic_2"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
    )
    .filter(F.col("enterprise_count") > 1)
    .orderBy(F.col("is_basic_2_dense_rank_tier").desc_nulls_first())
)
