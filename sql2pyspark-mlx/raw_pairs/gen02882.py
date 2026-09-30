from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency")
w_hist_2 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())
w3 = Window.partitionBy("plan", "currency")
w_hist_3 = Window.partitionBy("plan").orderBy(F.col("val").desc())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("val"),
    )
)

metrics = (
    exploded
    .select(
        "plan",
        "currency",
        "val",
        F.min("val").over(w_hist).alias("currency_min_val"),
        F.cume_dist().over(w_hist_2).alias("val_cume_dist"),
        F.stddev_samp("val").over(w_hist).alias("currency_stddev_samp_val"),
        F.count("val").over(w3).alias("plan_count_val"),
        F.cume_dist().over(w_hist_3).alias("val_cume_dist_2"),
    )
)

filtered = (
    metrics
    .filter(F.col("val_cume_dist_2").between(0.36, 0.54))
    .select(
        "plan",
        "currency",
        "val",
        "currency_min_val",
        "currency_stddev_samp_val",
        "val_cume_dist_2",
        F.floor("currency_stddev_samp_val").alias("currency_stddev_samp_val_whole"),
    )
)

filtered_price_tiers = (
    filtered
    .filter(F.col("currency_stddev_samp_val_whole").between(36, 53))
    .select(
        "currency",
        "val",
        "currency_min_val",
        "currency_stddev_samp_val",
        "currency_stddev_samp_val_whole",
        F.floor("currency_stddev_samp_val_whole").alias("currency_stddev_samp_val_whole_whole"),
    )
)

result = (
    filtered_price_tiers
    .groupBy(
        F.when(F.col("val") >= 62, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
        F.when(F.col("currency_stddev_samp_val_whole") >= 82, "PREMIUM").otherwise("STANDARD").alias("currency_stddev_samp_val_whole_tier"),
        "currency",
    )
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.avg("currency_stddev_samp_val").alias("avg_currency_stddev_samp_val"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("currency_stddev_samp_val_whole_whole")).otherwise(0)).alias("eur_currency_stddev_samp_val_whole_whole"),
        F.sort_array(F.collect_set("currency")).alias("currency_list_2"),
        F.percentile_approx("val", 0.9).alias("val_p90"),
    )
    .filter(F.col("val_p90") > 0)
)
