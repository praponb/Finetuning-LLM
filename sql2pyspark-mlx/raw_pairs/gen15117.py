from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("discount_pct")
w_running = Window.partitionBy("channel").orderBy("starts_on", "id")
w_recent = Window.partitionBy("channel").orderBy("discount_pct_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("promo_code", "channel").orderBy("ends_on_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("running_count_discount_pct_rank", "id")
w6 = Window.partitionBy("channel", "promo_code").orderBy("running_sum_discount_pct_rank", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy(F.year("ends_on_plus_months"), "channel").orderBy(F.col("running_sum_discount_pct_rank").desc())
w8 = Window.partitionBy("channel").orderBy(F.col("running_sum_discount_pct_rank_cume_dist").desc_nulls_first(), "id")
w9 = Window.partitionBy("channel").orderBy(F.col("running_count_discount_pct_rank_bucket").desc_nulls_first(), "id")

scored = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        F.rank().over(w).alias("discount_pct_rank"),
        F.lead("discount_pct", 1, 0).over(w_running).alias("next_discount_pct"),
    )
)

filtered = (
    scored
    .filter(F.col("ends_on") >= F.to_date(F.lit("2023-11-23")))
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct_rank",
        F.add_months("ends_on", 6).alias("ends_on_plus_months"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.sum("discount_pct_rank").over(w_recent).alias("running_sum_discount_pct_rank"),
        F.count("discount_pct_rank").over(w_recent).alias("running_count_discount_pct_rank"),
        F.max("discount_pct_rank").over(w_recent).alias("running_max_discount_pct_rank"),
        F.first("channel", ignorenulls=True).over(w_part).alias("first_known_channel"),
    )
)

base = (
    enriched
    .filter(F.col("running_sum_discount_pct_rank").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("promo_code")).alias("promo_code_title"),
        F.round((F.col("running_max_discount_pct_rank") / F.col("running_count_discount_pct_rank")), 2).alias("running_max_discount_pct_rank_per_running_count_discount_pct_rank"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct_rank",
        "running_sum_discount_pct_rank",
        "running_count_discount_pct_rank",
        "running_max_discount_pct_rank",
        "promo_code_title",
        "resolved_channel",
        F.ntile(3).over(w5).alias("running_count_discount_pct_rank_bucket"),
        F.avg("running_max_discount_pct_rank_per_running_count_discount_pct_rank").over(w6).alias("running_max_discount_pct_rank_per_running_count_discount_pct_rank_avg_6_prior"),
        F.cume_dist().over(w7).alias("running_sum_discount_pct_rank_cume_dist"),
    )
)

deduped = (
    with_windows
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w9))
    .filter(F.col("latest_rank") == 1)
)

daily = (
    latest
    .groupBy("promo_code")
    .agg(
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_rows"),
        F.min("running_max_discount_pct_rank").alias("min_running_max_discount_pct_rank"),
    )
    .filter(F.col("vip_rows") > 1)
)

result = (
    daily
    .select(
        "promo_code",
        "vip_rows",
        "min_running_max_discount_pct_rank",
        F.when(F.col("promo_code").rlike("^(WELCOME5|BUNDLE)$"), "EXTENDED").when(F.col("promo_code").rlike("^(SPRING10|VIP)$"), "GROUP_A").otherwise("GROUP_B").alias("promo_code_zone"),
    )
    .orderBy(F.col("vip_rows").asc_nulls_first())
)
