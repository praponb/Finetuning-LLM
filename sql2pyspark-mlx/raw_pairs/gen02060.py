from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("weeks_ends_on_to_starts_on", "id")
w2 = Window.orderBy("discount_pct")
w3 = Window.partitionBy(F.year("starts_on"), "promo_code").orderBy("ends_on", "id")
w4 = Window.partitionBy(F.date_trunc("WEEK", "ends_on"), "channel").orderBy("weeks_ends_on_to_starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promo_code", "channel")
w_recent = Window.partitionBy(F.year("ends_on"), "channel").orderBy(F.col("weeks_ends_on_to_starts_on").desc(), "id")

base = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-09-09").cast("date")) & (F.col("ends_on") > "2024-08-01"))
    .withColumn("channel_upper", F.upper("channel"))
    .withColumn("weeks_ends_on_to_starts_on", (F.datediff("starts_on", "ends_on") / 7))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("UNKNOWN")))
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .filter(~F.col("channel").isin(["mobile", "partner"]))
    .select(
        "*",
        F.dense_rank().over(w2).alias("discount_pct_dense_rank"),
        F.lag("discount_pct", 1).over(w3).alias("prev_discount_pct"),
    )
)

with_windows = (
    enriched
    .withColumn("first_known_prev_discount_pct", F.first("prev_discount_pct", ignorenulls=True).over(w4))
    .withColumn("prev_discount_pct_share", F.round((F.col("prev_discount_pct") / F.sum("prev_discount_pct").over(w5)), 4))
    .withColumn("row_seq", F.row_number().over(w_recent))
)

matched = (
    with_windows
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "channel_upper",
        "resolved_channel",
        "dedup_rank",
        "discount_pct_dense_rank",
    )
)
