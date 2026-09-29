from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w_recent = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")

filtered = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-07-11").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.trim("channel").alias("clean_channel"),
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered_promotions = (
    latest
    .filter(F.col("ends_on") >= F.to_date(F.lit("2023-11-11")))
    .withColumn("dedup_rank_per_discount_pct_int", (F.col("dedup_rank") / F.nullif("discount_pct_int", F.lit(0))))
    .withColumn("promo_code_title", F.initcap(F.trim("promo_code")))
)

base = (
    filtered_promotions
    .filter((F.col("ends_on") > F.lit("2023-10-23").cast("date")) & F.col("channel").isNotNull())
    .withColumn("channel_title", F.initcap(F.trim("channel")))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("NONE")))
)

metrics = (
    base
    .select(
        "id",
        "ends_on",
        "clean_channel",
        "dedup_rank",
        "dedup_rank_per_discount_pct_int",
        "promo_code_title",
        "channel_title",
        "resolved_channel",
        F.sum("discount_pct_int").over(w_recent).alias("last4_sum_discount_pct_int"),
        F.avg("discount_pct_int").over(w_recent).alias("last4_avg_discount_pct_int"),
        F.max("discount_pct_int").over(w_recent).alias("last4_max_discount_pct_int"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ends_on",
        "clean_channel",
        "dedup_rank",
        "dedup_rank_per_discount_pct_int",
        "promo_code_title",
        "last4_avg_discount_pct_int",
        "last4_max_discount_pct_int",
        "event_seq",
    )
)
