from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy(F.date_trunc("YEAR", "starts_on"), "channel")
w3 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("starts_on_period_sum_discount_pct")
w5 = Window.partitionBy("channel").orderBy(F.col("starts_on_period_sum_discount_pct").desc(), "id")
w6 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w7 = Window.partitionBy("channel", "promo_code").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("promo_code").orderBy("row_num", "id")

base = (
    df_promotions
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-09-22"))) & (F.col("ends_on") > "2025-07-01"))
    .withColumn("promo_code_channel_key", F.concat_ws("-", "promo_code", "channel"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("UNKNOWN")))
)

scored = (
    base
    .filter(F.year("starts_on") >= 2025)
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        "resolved_channel",
        F.row_number().over(w).alias("event_seq"),
        F.sum("discount_pct").over(w2).alias("starts_on_period_sum_discount_pct"),
        F.sum("discount_pct").over(w3).alias("cumulative_discount_pct"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    latest
    .withColumn("starts_on_period_sum_discount_pct_bucket", F.ntile(4).over(w5))
    .withColumn("discount_pct_bucket", F.ntile(10).over(w6))
    .withColumn("last_known_cumulative_discount_pct", F.last("cumulative_discount_pct", ignorenulls=True).over(w7))
)

scored_promotions = (
    with_windows
    .select(
        "id",
        "event_seq",
        "starts_on_period_sum_discount_pct",
        "cumulative_discount_pct",
        "row_num",
        "starts_on_period_sum_discount_pct_bucket",
        "discount_pct_bucket",
        "last_known_cumulative_discount_pct",
        F.lead("starts_on_period_sum_discount_pct_bucket").over(w_recent).alias("next_starts_on_period_sum_discount_pct_bucket"),
        F.lag("last_known_cumulative_discount_pct").over(w_recent).alias("prev_last_known_cumulative_discount_pct"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

result = (
    scored_promotions
    .select(
        "id",
        "starts_on_period_sum_discount_pct",
        "cumulative_discount_pct",
        "starts_on_period_sum_discount_pct_bucket",
        "discount_pct_bucket",
        "last_known_cumulative_discount_pct",
        "next_starts_on_period_sum_discount_pct_bucket",
        "prev_last_known_cumulative_discount_pct",
    )
    .orderBy(F.col("discount_pct_bucket").desc())
)
