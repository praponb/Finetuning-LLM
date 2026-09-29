from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id", "promo_id")
w2 = Window.orderBy("promo_id", "id", "promo_id")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id", "promo_id").rowsBetween(-2, -1)
w4 = Window.partitionBy("channel").orderBy(F.col("promo_id").desc(), "id", "promo_id")

valid = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-03-30").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.substring("channel", 1, 2).alias("channel_prefix"),
    )
)

cleaned = (
    valid
    .filter(F.col("channel").isNotNull() & (F.col("starts_on") > F.to_date(F.lit("2023-04-07"))))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "channel_prefix",
        F.add_months("starts_on", 1).alias("starts_on_plus_months"),
        F.length("channel").alias("channel_length"),
    )
)

with_window = (
    cleaned
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows = (
    with_window
    .withColumn("discount_pct_bucket", F.ntile(4).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("promotion_discount_pct_avg_2_prior", F.avg("promotion_discount_pct").over(w3))
)

base = (
    with_windows
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-03-24"))) & (F.col("promotion_promo_code").eqNullSafe(F.lit("WELCOME5"))))
    .withColumn("promotion_discount_pct_avg_2_prior_adjusted", F.round((F.col("promotion_discount_pct_avg_2_prior") * 1.15), 2))
    .withColumn("row_seq_plus_discount_pct", (F.col("row_seq") + F.col("discount_pct")))
    .withColumn("promotion_promo_code_filled", F.coalesce("promotion_promo_code", F.lit("UNKNOWN")))
)

ranked = (
    base
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "promo_code",
        "discount_pct",
        "starts_on_plus_months",
        "channel_length",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct_avg_2_prior",
        "promotion_discount_pct_avg_2_prior_adjusted",
        "promotion_promo_code_filled",
    )
)

result = (
    ranked
    .select(
        "id",
        "promo_code",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct_avg_2_prior",
        "promotion_discount_pct_avg_2_prior_adjusted",
        "promotion_promo_code_filled",
        F.when(F.col("promotion_promo_code").rlike("^(BUNDLE|WELCOME5)$"), "GROUP_A").when(F.col("promotion_promo_code").rlike("^(SPRING10|VIP)$"), "CORE").otherwise("PRIORITY").alias("promotion_promo_code_zone"),
    )
    .orderBy("promo_code")
)
