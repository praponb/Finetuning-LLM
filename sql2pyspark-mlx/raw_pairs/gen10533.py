from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("ends_on", "id")
w3 = Window.partitionBy("promo_code").orderBy("is_spring10", "id")

ranked = (
    df_promotions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked_promotions = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "promo_code")
)

valid = (
    ranked_promotions
    .filter(F.col("promo_code").isin(["WELCOME5", "BUNDLE", "SPRING10"]))
    .select(
        "*",
        F.when(F.col("promo_code") == "SPRING10", 1).otherwise(0).alias("is_spring10"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    latest
    .filter(F.col("is_spring10") <= 46)
    .select(
        "*",
        F.when(F.col("promo_code").isin(["WELCOME5", "SPRING10"]), "Tier 1").otherwise("Other").alias("promo_code_group"),
        F.least("is_spring10", "latest_rank").alias("least_is_spring10_latest_rank"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .subtract(
        df_promotions
        .filter(F.col("channel").isin(["store", "partner", "mobile"]))
        .select("id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

stats = (
    verified
    .groupBy(
        F.when(F.col("is_spring10") >= 87, "PREMIUM").otherwise("STANDARD").alias("is_spring10_tier"),
        F.when(F.col("least_is_spring10_latest_rank") >= 64, "PREMIUM").otherwise("STANDARD").alias("least_is_spring10_latest_rank_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.percentile_approx("is_spring10", [0.25, 0.5, 0.75]).alias("is_spring10_quartiles"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
    .filter(F.col("num_promo_codes") > 1)
)

result = (
    stats
    .select(
        "is_spring10_tier",
        "least_is_spring10_latest_rank_tier",
        "promo_code_list",
        "is_spring10_quartiles",
        "num_promo_codes",
    )
    .orderBy(F.col("least_is_spring10_latest_rank_tier").desc_nulls_last())
)
