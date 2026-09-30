from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("months_ends_on_to_starts_on").asc_nulls_last())
w3 = Window.orderBy("starts_on_month_end", "id")

base = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2023-09-16").cast("date"))
    .withColumn("months_ends_on_to_starts_on", F.round((F.datediff("starts_on", "ends_on") / 30), 1))
    .withColumn("starts_on_month_end", F.last_day("starts_on"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("DIRECT")))
)

with_windows = (
    base
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w))
    .withColumn("months_ends_on_to_starts_on_percent_rank", F.percent_rank().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

with_window = (
    with_windows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

filtered = (
    with_window
    .filter((F.col("ends_on") >= F.lit("2023-10-07").cast("date")) & (F.col("promo_id") < 8))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "months_ends_on_to_starts_on",
        "starts_on_month_end",
        "months_ends_on_to_starts_on_percent_rank",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        F.trim("promo_code").alias("clean_promo_code"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

cleaned = (
    filtered
    .filter((F.col("ends_on") > F.lit("2023-08-04").cast("date")) & (F.col("channel").eqNullSafe(F.lit("store"))))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "months_ends_on_to_starts_on",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "clean_promo_code",
        "channel_clean",
        F.concat_ws("-", "channel", "promo_code").alias("channel_promo_code_key"),
        F.coalesce("promotion_promo_code", F.lit("NONE")).alias("promotion_promo_code_clean"),
    )
)

result = (
    cleaned
    .groupBy("promo_id")
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("promo_id"))).alias("partner_total_promo_id"),
        F.count(F.when(F.col("promotion_promo_code") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.percentile_approx("promotion_discount_pct", 0.5).alias("promotion_discount_pct_p50"),
    )
)
