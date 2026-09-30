from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("tier_value").desc())

exploded = df_price_tiers.select("id", F.explode_outer("tier_prices").alias("tier_value"))

scored = (
    exploded
    .filter(F.col("tier_value") > 62)
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w_running))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w_running))
    .withColumn("tier_value_dense_rank_2", F.dense_rank().over(w_running))
)

cleaned = (
    scored
    .filter(F.col("tier_value").isNotNull())
    .withColumn("tier_value_percent_rank_int", F.col("tier_value_percent_rank").cast("int"))
    .withColumn("tier_value_rounded", F.round("tier_value", 2))
)

filtered = (
    cleaned
    .filter(F.col("tier_value_percent_rank") >= 0.22)
    .select(
        "tier_value",
        "tier_value_dense_rank",
        "tier_value_percent_rank",
        "tier_value_dense_rank_2",
        "tier_value_percent_rank_int",
        "tier_value_rounded",
        (F.col("tier_value_dense_rank_2") - F.col("tier_value_dense_rank")).alias("tier_value_dense_rank_2_minus_tier_value_dense_rank"),
    )
)

result = (
    filtered
    .withColumn("tier_value_dense_rank_2_minus_tier_value_dense_rank_tier", F.when(F.col("tier_value_dense_rank_2_minus_tier_value_dense_rank") >= 68, "HIGH").otherwise("LOW"))
    .withColumn("tier_value_percent_rank_int_tier", F.when(F.col("tier_value_percent_rank_int") >= 82, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["tier_value_dense_rank_2_minus_tier_value_dense_rank_tier", "tier_value_percent_rank_int_tier"], ["tier_value_dense_rank_2_minus_tier_value_dense_rank_tier"]],
        "tier_value_dense_rank_2_minus_tier_value_dense_rank_tier",
        "tier_value_percent_rank_int_tier",
    )
    .agg(
        F.percentile_approx("tier_value_percent_rank_int", 0.5).alias("tier_value_percent_rank_int_p50"),
        F.percentile_approx("tier_value_percent_rank_int", [0.25, 0.5, 0.75]).alias("tier_value_percent_rank_int_quartiles"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("tier_value_dense_rank_2_minus_tier_value_dense_rank_tier").desc())
)
