from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct")
w3 = Window.partitionBy("channel", "promo_code")
w4 = Window.partitionBy("channel")
w_recent = Window.orderBy(F.col("running_discount_pct_whole").desc_nulls_first())

cleaned = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-07-17").cast("date")) & (F.col("ends_on") > "2023-05-01"))
    .select(
        "*",
        F.concat("promo_code", F.lit(", "), "channel").alias("promo_code_channel"),
    )
)

with_windows = (
    cleaned
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w_ordered))
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w2))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4))
)

remaining_keys = (
    with_windows
    .select("channel", "promo_code")
    .subtract(
        df_promotions
        .filter(F.col("promo_code").isin(["BUNDLE", "WELCOME5", "SPRING10"]))
        .select("channel", "promo_code")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter((F.col("ends_on") >= F.lit("2023-10-12").cast("date")) & F.col("channel").isNotNull())
    .select(
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "running_discount_pct",
        "discount_pct_percent_rank",
        "discount_pct_share",
        F.floor("running_discount_pct").alias("running_discount_pct_whole"),
    )
)

valid = (
    base
    .filter(F.col("ends_on") >= F.lit("2024-04-23").cast("date"))
    .select(
        "*",
        F.date_trunc("WEEK", "starts_on").alias("starts_on_period"),
        F.substring("channel", 1, 3).alias("channel_prefix"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.round((F.col("running_discount_pct") / F.sum("running_discount_pct").over(w4)), 4).alias("running_discount_pct_share"),
        F.cume_dist().over(w_recent).alias("running_discount_pct_whole_cume_dist"),
        F.max("running_discount_pct_whole").over(w4).alias("channel_max_running_discount_pct_whole"),
    )
)

with_window = (
    metrics
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    with_window
    .select(
        "channel",
        "starts_on",
        "ends_on",
        "running_discount_pct",
        "discount_pct_percent_rank",
        "running_discount_pct_whole",
        "channel_prefix",
        "running_discount_pct_whole_cume_dist",
        "channel_max_running_discount_pct_whole",
    )
    .orderBy(F.col("discount_pct_percent_rank").desc_nulls_first())
)
