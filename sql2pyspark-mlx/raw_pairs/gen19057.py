from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("days_since_starts_on", "id")
w3 = Window.partitionBy("promo_code").orderBy(F.col("days_since_starts_on").desc_nulls_last(), "id")
w_recent = Window.partitionBy("promo_code").orderBy("latest_rank", "id")
w5 = Window.partitionBy(F.date_trunc("YEAR", "ends_on"), "promo_code")

filtered = (
    df_promotions
    .filter(F.year("ends_on") >= 2025)
    .select(
        "*",
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
        F.initcap(F.trim("promo_code")).alias("promo_code_title"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "promo_code",
        "ends_on",
        "days_since_starts_on",
        "channel_filled",
        F.sum("discount_pct").over(w_ordered).alias("running_sum_discount_pct"),
        F.avg("discount_pct").over(w_ordered).alias("running_avg_discount_pct"),
        F.min("discount_pct").over(w_ordered).alias("running_min_discount_pct"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

base = (
    enriched
    .filter(F.col("running_min_discount_pct") < 0.14)
    .select("*", F.trim("promo_code").alias("clean_promo_code"))
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "promo_code",
        "ends_on",
        "channel_filled",
        "running_sum_discount_pct",
        "running_avg_discount_pct",
        "running_min_discount_pct",
        "seq_no",
        "latest_rank",
        F.lag("running_min_discount_pct").over(w_recent).alias("prev_running_min_discount_pct"),
        F.lead("running_min_discount_pct").over(w_recent).alias("next_running_min_discount_pct"),
        F.lead("days_since_starts_on").over(w_recent).alias("next_days_since_starts_on"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.round((F.col("running_min_discount_pct") / F.sum("running_min_discount_pct").over(w5)), 4).alias("running_min_discount_pct_share"),
    )
)
