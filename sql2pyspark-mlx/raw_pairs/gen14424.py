from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "ends_on"), "promo_code")

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.year("starts_on") >= 2023))
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.lower("channel").alias("channel_lower"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.lag("discount_pct").over(w).alias("prev_discount_pct"),
        F.lead("discount_pct").over(w).alias("next_discount_pct"),
        F.row_number().over(w).alias("row_seq"),
        F.sum("discount_pct").over(w2).alias("ends_on_period_sum_discount_pct"),
    )
)

base = (
    scored
    .filter(F.col("ends_on") >= F.to_date(F.lit("2023-08-26")))
    .select(
        "id",
        "promo_code",
        "ends_on",
        "channel_lower",
        "prev_discount_pct",
        "next_discount_pct",
        "ends_on_period_sum_discount_pct",
        F.trim("promo_code").alias("clean_promo_code"),
    )
)

cleaned = (
    base
    .filter(F.month("ends_on").isin([1, 6, 8]))
    .withColumn("ends_on_period_sum_discount_pct_rounded", F.round("ends_on_period_sum_discount_pct", 0))
)

result = (
    cleaned
    .select(
        "id",
        "promo_code",
        "ends_on",
        "prev_discount_pct",
        "ends_on_period_sum_discount_pct",
        "clean_promo_code",
        F.when(F.col("promo_code").rlike("^(WELCOME5|BUNDLE)$"), "GROUP_A").when(F.col("promo_code").rlike("^(SPRING10|VIP)$"), "OTHER_ZONE").otherwise("CORE").alias("promo_code_zone"),
    )
)
