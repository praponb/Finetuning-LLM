from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2023-11-30").cast("date"))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.round("discount_pct", 0).alias("discount_pct_rounded"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("MONTH", "starts_on").alias("activity_month"))
    .agg(
        F.max("discount_pct_rounded").alias("max_discount_pct_rounded"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.percentile_approx("discount_pct_rounded", [0.25, 0.5, 0.75]).alias("discount_pct_rounded_quartiles"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
    .orderBy(F.col("num_channels").asc_nulls_first())
)
