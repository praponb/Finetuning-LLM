from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("discount_pct_tier").orderBy(F.col("adjusted_discount_pct_int_p50").desc())

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("ends_on") >= "2025-03-07"))
    .select(
        "*",
        F.round((F.col("discount_pct") * 1.15), 2).alias("adjusted_discount_pct"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

valid = (
    cleaned
    .filter((F.col("channel").eqNullSafe(F.lit("partner"))) & (F.col("starts_on") > F.to_date(F.lit("2023-07-01"))))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "adjusted_discount_pct",
        "channel_filled",
        (F.col("adjusted_discount_pct") + F.col("discount_pct")).alias("adjusted_discount_pct_plus_discount_pct"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

valid_promotions = (
    valid
    .filter(F.col("ends_on") > F.lit("2023-08-09").cast("date"))
    .select(
        "*",
        F.col("adjusted_discount_pct").cast("int").alias("adjusted_discount_pct_int"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean_2"),
    )
)

base = (
    valid_promotions
    .filter(F.col("adjusted_discount_pct_plus_discount_pct") > 32)
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "adjusted_discount_pct",
        "channel_filled",
        "adjusted_discount_pct_plus_discount_pct",
        "channel_clean",
        "adjusted_discount_pct_int",
        "channel_clean_2",
        F.floor("adjusted_discount_pct_plus_discount_pct").alias("adjusted_discount_pct_plus_discount_pct_whole"),
    )
)

base_promotions = (
    base
    .filter(F.col("ends_on") >= F.lit("2023-10-15").cast("date"))
    .select("*", F.date_trunc("month", "ends_on").alias("ends_on_period"))
)

result = (
    base_promotions
    .groupBy(
        F.when(F.col("discount_pct") >= 0.29, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
        F.when(F.col("adjusted_discount_pct_plus_discount_pct") >= 76, "PREMIUM").otherwise("STANDARD").alias("adjusted_discount_pct_plus_discount_pct_tier"),
    )
    .agg(
        F.percentile_approx("adjusted_discount_pct_plus_discount_pct_whole", [0.25, 0.5, 0.75]).alias("adjusted_discount_pct_plus_discount_pct_whole_quartiles"),
        F.percentile_approx("adjusted_discount_pct_int", 0.5).alias("adjusted_discount_pct_int_p50"),
    )
    .withColumn("adjusted_discount_pct_int_p50_rank", F.rank().over(w))
    .orderBy(F.col("discount_pct_tier").desc())
)
