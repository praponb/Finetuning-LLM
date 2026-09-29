from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("currency")
w2 = Window.partitionBy("currency", "plan")
w3 = Window.orderBy(F.col("val_adjusted").desc_nulls_last())
w4 = Window.orderBy(F.col("val").asc_nulls_first())
w_recent = Window.orderBy(F.col("currency_min_val").asc_nulls_last())
w6 = Window.orderBy(F.col("val_adjusted_dense_rank_str").desc())
w7 = Window.orderBy(F.col("val_rank").desc())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("val"))
)

filtered = (
    exploded
    .filter(F.col("val") != 69)
    .withColumn("val_adjusted", F.round((F.col("val") * 100), 2))
)

scored = (
    filtered
    .select(
        "id",
        "plan",
        "val",
        F.count("val_adjusted").over(w_grp).alias("currency_count_val_adjusted"),
        F.min("val").over(w2).alias("currency_min_val"),
        F.dense_rank().over(w3).alias("val_adjusted_dense_rank"),
        F.rank().over(w4).alias("val_rank"),
    )
)

filtered_price_tiers = (
    scored
    .filter(F.col("currency_min_val") == 67)
    .select(
        "id",
        "plan",
        "currency_count_val_adjusted",
        "currency_min_val",
        "val_adjusted_dense_rank",
        "val_rank",
        F.col("val_adjusted_dense_rank").cast("double").alias("val_adjusted_dense_rank_str"),
    )
)

filtered_price_tiers_2 = (
    filtered_price_tiers
    .filter(F.col("val_adjusted_dense_rank_str").isNotNull())
    .select(
        "id",
        "currency_min_val",
        "val_adjusted_dense_rank",
        "val_rank",
        "val_adjusted_dense_rank_str",
        F.when(F.col("val_rank") >= 9, "High").when(F.col("val_rank") >= 4, "Medium").otherwise("Low").alias("val_rank_level"),
    )
)

metrics = (
    filtered_price_tiers_2
    .select(
        "id",
        "val_adjusted_dense_rank",
        "val_adjusted_dense_rank_str",
        "val_rank_level",
        F.percent_rank().over(w_recent).alias("currency_min_val_percent_rank"),
        F.cume_dist().over(w6).alias("val_adjusted_dense_rank_str_cume_dist"),
        F.percent_rank().over(w7).alias("val_rank_percent_rank"),
    )
)

result = (
    metrics
    .withColumn("currency_min_val_percent_rank_tier", F.when(F.col("currency_min_val_percent_rank") >= 0.72, "HIGH").otherwise("LOW"))
    .withColumn("val_adjusted_dense_rank_str_cume_dist_tier", F.when(F.col("val_adjusted_dense_rank_str_cume_dist") >= 0.83, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["currency_min_val_percent_rank_tier", "val_adjusted_dense_rank_str_cume_dist_tier"], ["currency_min_val_percent_rank_tier"], []],
        "currency_min_val_percent_rank_tier",
        "val_adjusted_dense_rank_str_cume_dist_tier",
    )
    .agg(
        F.percentile_approx("val_rank_percent_rank", 0.5).alias("val_rank_percent_rank_p50"),
        F.percentile_approx("val_adjusted_dense_rank_str_cume_dist", 0.95).alias("val_adjusted_dense_rank_str_cume_dist_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("currency_min_val_percent_rank"), 2).alias("avg_currency_min_val_percent_rank"),
        F.count("*").alias("cnt"),
        F.sum("val_adjusted_dense_rank").alias("total_val_adjusted_dense_rank"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("val_adjusted_dense_rank_str_cume_dist_p95").desc())
)
