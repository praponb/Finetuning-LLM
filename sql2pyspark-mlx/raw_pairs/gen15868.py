from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-09-20").cast("date")) & F.col("channel").isNotNull())
    .select("id", "promo_code", F.trim("promo_code").alias("clean_promo_code"))
)

base = (
    cleaned
    .filter(F.col("promo_code") != "VIP")
    .select(
        "*",
        F.when(F.col("promo_code") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
    )
)

base_promotions = (
    base
    .filter(F.col("is_welcome5") > 68)
    .withColumn("is_welcome5_tier", F.when(F.col("is_welcome5") >= 88, "High").when(F.col("is_welcome5") >= 27, "Medium").otherwise("Low"))
)

filtered = (
    base_promotions
    .filter(F.col("is_welcome5") != 22)
    .select(
        "id",
        "promo_code",
        "is_welcome5",
        "is_welcome5_tier",
        F.initcap(F.trim("promo_code")).alias("promo_code_title"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("is_welcome5") >= 90, "PREMIUM").otherwise("STANDARD").alias("is_welcome5_tier"),
        "promo_code",
    )
    .agg(
        F.percentile_approx("is_welcome5", [0.25, 0.5, 0.75]).alias("is_welcome5_quartiles"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.round(F.avg("is_welcome5"), 2).alias("avg_is_welcome5"),
        F.avg("is_welcome5").alias("avg_is_welcome5_2"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes_2"),
    )
    .orderBy(F.col("avg_is_welcome5_2").desc())
)
