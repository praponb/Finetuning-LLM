from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("promo_code").orderBy("starts_on_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("starts_on_plus_months"), "channel").orderBy("starts_on_year", "id")
w3 = Window.partitionBy(F.date_trunc("MONTH", "starts_on_plus_months"), "channel").orderBy(F.col("running_count_starts_on_year").desc(), "id")
w4 = Window.partitionBy(F.year("starts_on_plus_months"), "channel").orderBy("days_starts_on_to_ends_on", "id")

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("starts_on_plus_months", F.add_months("starts_on", 1))
    .withColumn("starts_on_year", F.year("starts_on"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("DIRECT")))
)

base = (
    filtered
    .filter(F.col("channel").isNotNull() & (F.col("starts_on_plus_months") > "2023-05-01"))
    .select(
        "*",
        F.length("channel").alias("channel_length"),
        F.datediff("ends_on", "starts_on").alias("days_starts_on_to_ends_on"),
    )
)

with_windows = (
    base
    .filter(F.col("starts_on_year") <= 79)
    .select(
        "id",
        "channel",
        "starts_on_plus_months",
        "starts_on_year",
        "channel_clean",
        "days_starts_on_to_ends_on",
        F.avg("starts_on_year").over(w_running).alias("running_avg_starts_on_year"),
        F.count("starts_on_year").over(w_running).alias("running_count_starts_on_year"),
        F.max("starts_on_year").over(w_running).alias("running_max_starts_on_year"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(4).over(w3).alias("running_count_starts_on_year_bucket"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

matched = (
    scored
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.starts_on_plus_months") >= F.col("pr.starts_on")) & (F.col("src.starts_on_plus_months") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .select(
        "id",
        "channel",
        "starts_on_plus_months",
        "starts_on_year",
        "days_starts_on_to_ends_on",
        "running_max_starts_on_year",
        "running_count_starts_on_year_bucket",
        "promo_id",
        "discount_pct",
        F.when(F.col("channel").rlike("^(partner|web)$"), "OTHER_ZONE").when(F.col("channel").rlike("^(mobile|store)$"), "EXTENDED").otherwise("GROUP_A").alias("channel_zone"),
    )
    .orderBy(F.col("channel_zone").desc_nulls_first())
)
