from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.partitionBy(F.year("ends_on"), "promo_code").orderBy("last4_sum_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("promo_code").orderBy("last4_sum_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("ends_on", "id")

valid = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-06-30").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.lower("channel").alias("channel_lower"),
        F.when(F.col("promo_code").isin(["WELCOME5", "VIP"]), "Tier 1").otherwise("Other").alias("promo_code_group"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.avg("discount_pct").over(w).alias("last4_avg_discount_pct"),
        F.count("discount_pct").over(w).alias("last4_count_discount_pct"),
        F.sum("discount_pct").over(w).alias("last4_sum_discount_pct"),
        F.min("discount_pct").over(w).alias("last4_min_discount_pct"),
    )
)

enriched_promotions = (
    enriched
    .select(
        "id",
        "ends_on",
        "channel_lower",
        "promo_code_group",
        "resolved_channel",
        "last4_avg_discount_pct",
        "last4_count_discount_pct",
        "last4_min_discount_pct",
        F.first("last4_min_discount_pct", ignorenulls=True).over(w_running).alias("first_known_last4_min_discount_pct"),
        F.first("last4_sum_discount_pct", ignorenulls=True).over(w_hist).alias("first_known_last4_sum_discount_pct"),
    )
)

with_windows = (
    enriched_promotions
    .select(
        "*",
        F.lag("first_known_last4_sum_discount_pct").over(w4).alias("prev_first_known_last4_sum_discount_pct"),
        F.lag("first_known_last4_min_discount_pct").over(w4).alias("prev_first_known_last4_min_discount_pct"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("first_known_last4_sum_discount_pct") >= 1.68, "LARGE").otherwise("SMALL").alias("first_known_last4_sum_discount_pct_tier"),
        F.date_trunc("MONTH", "ends_on").alias("activity_month"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.round(F.avg("first_known_last4_sum_discount_pct"), 2).alias("avg_first_known_last4_sum_discount_pct"),
        F.min("first_known_last4_sum_discount_pct").alias("min_first_known_last4_sum_discount_pct"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
    )
    .orderBy("row_count")
)
