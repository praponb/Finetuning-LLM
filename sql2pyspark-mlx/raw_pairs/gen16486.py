from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("channel")
w2 = Window.partitionBy("promo_code")

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.concat("channel", F.lit(" "), "promo_code").alias("channel_promo_code"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "channel_promo_code",
        F.max("discount_pct").over(w_running).alias("channel_max_discount_pct"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
        F.stddev_samp("discount_pct").over(w2).alias("promo_code_stddev_samp_discount_pct"),
    )
)

base = (
    with_windows
    .filter(F.col("promo_code") != "SPRING10")
    .select(
        "*",
        (F.col("promo_code_stddev_samp_discount_pct") + F.col("channel_max_discount_pct")).alias("promo_code_stddev_samp_discount_pct_plus_channel_max_discount_pct"),
        F.round("discount_pct", 1).alias("discount_pct_rounded"),
    )
)

base_promotions = (
    base
    .filter(F.col("ends_on") > "2023-08-23")
    .withColumn("promo_code_stddev_samp_discount_pct_times_discount_pct_rounded", (F.col("promo_code_stddev_samp_discount_pct") * F.col("discount_pct_rounded")))
    .withColumn("clean_promo_code", F.trim("promo_code"))
)

valid_promotions = (
    base_promotions
    .filter((F.col("ends_on") > F.lit("2023-08-21").cast("date")) & (F.col("promo_code_stddev_samp_discount_pct_times_discount_pct_rounded") <= 53))
    .select("*", F.date_format("ends_on", "MMM yyyy").alias("ends_on_label"))
)

result = (
    valid_promotions
    .groupBy(
        F.when(F.col("promo_code_stddev_samp_discount_pct") >= 0, "PREMIUM").otherwise("STANDARD").alias("promo_code_stddev_samp_discount_pct_tier"),
    )
    .agg(
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("discount_pct_per_welcome5"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.avg("promo_code_stddev_samp_discount_pct_times_discount_pct_rounded").alias("avg_promo_code_stddev_samp_discount_pct_times_discount_pct_rounded"),
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("discount_pct"))).alias("bundle_total_discount_pct"),
        F.avg("discount_pct_share").alias("avg_discount_pct_share"),
    )
    .filter(F.col("bundle_total_discount_pct") < 1000000)
)
