from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "ends_on"), "promo_code").orderBy(F.col("weeks_starts_on_to_ends_on").desc_nulls_first())
w2 = Window.partitionBy(F.month("starts_on"), "channel")
w3 = Window.partitionBy(F.month("ends_on"), "promo_code")
w4 = Window.partitionBy("channel").orderBy(F.col("weeks_starts_on_to_ends_on").desc_nulls_first())
w_running = Window.partitionBy("channel")

valid = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2023-08-22").cast("date"))
    .select("*", F.lower("promo_code").alias("promo_code_lower"))
)

matched_keys = (
    valid
    .select("promo_code")
    .subtract(
        df_promotions
        .filter(F.col("promo_code").isin(["SPRING10", "WELCOME5", "BUNDLE"]))
        .select("promo_code")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("channel").isNotNull() & (F.col("starts_on") > F.lit("2023-04-23").cast("date")))
    .withColumn("weeks_starts_on_to_ends_on", (F.datediff("ends_on", "starts_on") / 7))
)

enriched = (
    filtered
    .select(
        "channel",
        "discount_pct",
        "weeks_starts_on_to_ends_on",
        F.rank().over(w).alias("weeks_starts_on_to_ends_on_rank"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
        F.stddev_samp("weeks_starts_on_to_ends_on").over(w3).alias("ends_on_month_stddev_samp_weeks_starts_on_to_ends_on"),
    )
)

scored = (
    enriched
    .withColumn("weeks_starts_on_to_ends_on_percent_rank", F.percent_rank().over(w4))
    .withColumn("channel_max_ends_on_month_stddev_samp_weeks_starts_on_to_ends_on", F.max("ends_on_month_stddev_samp_weeks_starts_on_to_ends_on").over(w_running))
    .withColumn("channel_count_weeks_starts_on_to_ends_on_rank", F.count("weeks_starts_on_to_ends_on_rank").over(w_running))
)

result = (
    scored
    .select(
        "channel",
        "weeks_starts_on_to_ends_on",
        "weeks_starts_on_to_ends_on_percent_rank",
    )
    .unpivot(
        ids=["channel"],
        values=["weeks_starts_on_to_ends_on", "weeks_starts_on_to_ends_on_percent_rank"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
