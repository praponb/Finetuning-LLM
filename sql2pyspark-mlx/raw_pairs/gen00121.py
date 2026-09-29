from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w2 = Window.partitionBy("promo_code").orderBy("ends_on_period")

valid = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-05-06").cast("date")) & F.col("channel").isNotNull())
    .withColumn("promo_code_upper", F.upper("promo_code"))
    .withColumn("is_partner", F.when(F.col("channel") == "partner", 1).otherwise(0))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("OTHER")))
)

base = (
    valid
    .filter(F.col("promo_code").isin(["VIP", "BUNDLE", "SPRING10"]))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "promo_code_upper",
        "is_partner",
        "channel_clean",
        F.date_trunc("YEAR", "ends_on").alias("ends_on_period"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

deduped = (
    latest
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "promo_code_upper",
        "channel_clean",
    )
)

result = (
    deduped
    .cube(
        F.when(F.col("discount_pct") >= 0.36, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
    )
    .orderBy("discount_pct_tier")
)
