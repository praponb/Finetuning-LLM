from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("channel", "promo_code").orderBy("is_bundle", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("ends_on"), "channel").orderBy(F.col("discount_pct").desc())
w4 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy(F.col("discount_pct_rounded").desc(), "id")
w_running = Window.partitionBy("promo_code", "channel").orderBy("last_known_is_bundle_2_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("channel")

base = (
    df_promotions
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-05-23"))) & F.col("channel").isNotNull())
    .select(
        "*",
        F.when(F.col("promo_code") == "BUNDLE", 1).otherwise(0).alias("is_bundle"),
        F.round("discount_pct", 1).alias("discount_pct_rounded"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.last("is_bundle", ignorenulls=True).over(w_ordered).alias("last_known_is_bundle"),
        F.last("is_bundle", ignorenulls=True).over(w_recent).alias("last_known_is_bundle_2"),
        F.percent_rank().over(w3).alias("discount_pct_percent_rank"),
        F.last("channel", ignorenulls=True).over(w4).alias("last_known_channel"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

valid = (
    latest
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-10-16"))) & (F.col("is_bundle") <= 30))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "is_bundle",
        "resolved_channel",
        "last_known_is_bundle_2",
        "row_num",
        F.round("last_known_is_bundle_2", 1).alias("last_known_is_bundle_2_rounded"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.sum("is_bundle").over(w_running).alias("running_sum_is_bundle"),
        F.max("is_bundle").over(w_running).alias("running_max_is_bundle"),
        F.min("is_bundle").over(w_running).alias("running_min_is_bundle"),
        F.max("last_known_is_bundle_2").over(w7).alias("channel_max_last_known_is_bundle_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "is_bundle",
        "resolved_channel",
        "last_known_is_bundle_2",
        "last_known_is_bundle_2_rounded",
        "running_max_is_bundle",
        F.when(F.col("promo_code").rlike("^(WELCOME5|VIP)$"), "EXTENDED").when(F.col("promo_code").rlike("^(SPRING10|BUNDLE)$"), "GROUP_A").otherwise("CORE").alias("promo_code_zone"),
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
