from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w2 = Window.orderBy(F.col("val_cume_dist").desc_nulls_first())
w3 = Window.orderBy(F.col("val").desc_nulls_last())
w4 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("val"))
)

scored = lines.withColumn("val_cume_dist", F.cume_dist().over(w))

matched_keys = (
    scored
    .select("currency", "id")
    .subtract(
        df_price_tiers
        .filter(F.col("currency") != "EUR")
        .select("currency", "id")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("currency").isin(["USD", "EUR", "THB"]))
    .withColumn("currency_lower", F.lower("currency"))
)

enriched = (
    base
    .filter(~F.col("plan").isin(["starter", "enterprise", "pro"]))
    .withColumn("val_cume_dist_dense_rank", F.dense_rank().over(w2))
    .withColumn("val_dense_rank", F.dense_rank().over(w3))
    .withColumn("currency_stddev_samp_val_cume_dist", F.stddev_samp("val_cume_dist").over(w4))
)

valid = (
    enriched
    .filter(F.col("currency_stddev_samp_val_cume_dist") > 1)
    .select("*", F.length("plan").alias("plan_length"))
)

cleaned = (
    valid
    .filter(F.col("val_dense_rank") >= 3)
    .select(
        "id",
        "plan",
        "currency",
        "val",
        "val_cume_dist_dense_rank",
        "val_dense_rank",
        "currency_stddev_samp_val_cume_dist",
        F.ceil("currency_stddev_samp_val_cume_dist").alias("currency_stddev_samp_val_cume_dist_whole"),
        (F.col("val_cume_dist_dense_rank") - F.col("currency_stddev_samp_val_cume_dist")).alias("val_cume_dist_dense_rank_minus_currency_stddev_samp_val_cume_dist"),
    )
)

daily = (
    cleaned
    .groupBy(
        "currency",
        F.when(F.col("val_cume_dist_dense_rank_minus_currency_stddev_samp_val_cume_dist") >= 80, "LARGE").otherwise("SMALL").alias("val_cume_dist_dense_rank_minus_currency_stddev_samp_val_cume_dist_tier"),
    )
    .agg(
        F.round(F.avg("currency_stddev_samp_val_cume_dist"), 2).alias("avg_currency_stddev_samp_val_cume_dist"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.round(F.avg("val_dense_rank"), 2).alias("avg_val_dense_rank"),
        F.sum(F.when(F.col("plan") == "pro", F.col("val_cume_dist_dense_rank_minus_currency_stddev_samp_val_cume_dist"))).alias("pro_total_val_cume_dist_dense_rank_minus_currency_stddev_samp_val_cume_dist"),
    )
)

result = (
    daily
    .select(
        "currency",
        "val_cume_dist_dense_rank_minus_currency_stddev_samp_val_cume_dist_tier",
        "avg_currency_stddev_samp_val_cume_dist",
        "enterprise_count",
        "distinct_ids",
        "avg_val_dense_rank",
        F.when(F.col("currency").rlike("^(EUR|THB)$"), "PRIORITY").when(F.col("currency").rlike("^(USD)$"), "GROUP_A").otherwise("EXTENDED").alias("currency_zone"),
    )
    .orderBy("distinct_ids", "avg_currency_stddev_samp_val_cume_dist")
)
