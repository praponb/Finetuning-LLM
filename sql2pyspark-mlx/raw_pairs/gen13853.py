from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("element_scaled_minus_element").asc_nulls_first())
w3 = Window.orderBy(F.col("element").asc_nulls_last())

lines = df_price_tiers.select("id", "currency", F.explode("tier_prices").alias("element"))

cleaned = (
    lines
    .filter(F.col("element") < 50)
    .withColumn("element_scaled", F.round((F.col("element") * 1.15), 2))
    .withColumn("currency_upper", F.upper("currency"))
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("element") <= 28)
    .select(
        "id",
        "currency",
        "element",
        "element_scaled",
        (F.col("element_scaled") - F.col("element")).alias("element_scaled_minus_element"),
    )
)

cleaned_price_tiers_2 = (
    cleaned_price_tiers
    .filter(F.col("element_scaled") <= 40)
    .select(
        "*",
        F.coalesce("element_scaled_minus_element", F.lit(0)).alias("element_scaled_minus_element_filled"),
        F.round((F.col("element_scaled_minus_element") * 1.15), 2).alias("element_scaled_minus_element_scaled"),
    )
)

cleaned_price_tiers_3 = (
    cleaned_price_tiers_2
    .filter(F.col("element_scaled_minus_element").between(39, 71))
    .select(
        "*",
        (F.col("element_scaled_minus_element_scaled") - F.col("element_scaled")).alias("element_scaled_minus_element_scaled_minus_element_scaled"),
    )
)

kept_keys = (
    cleaned_price_tiers_3
    .select("id", "currency")
    .exceptAll(
        df_price_tiers
        .filter(F.col("plan") == "enterprise")
        .select("id", "currency")
    )
)

reconciled = (
    cleaned_price_tiers_3
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "element",
        "element_scaled",
        "element_scaled_minus_element",
        "element_scaled_minus_element_scaled",
        F.max("element_scaled_minus_element_scaled_minus_element_scaled").over(w_running).alias("currency_max_element_scaled_minus_element_scaled_minus_element_scaled"),
        F.stddev_samp("element").over(w_running).alias("currency_stddev_samp_element"),
        F.rank().over(w2).alias("element_scaled_minus_element_rank"),
        F.dense_rank().over(w3).alias("element_dense_rank"),
    )
)

daily = (
    metrics
    .withColumn("currency_stddev_samp_element_tier", F.when(F.col("currency_stddev_samp_element") >= 68, "PREMIUM").otherwise("STANDARD"))
    .withColumn("element_scaled_minus_element_tier", F.when(F.col("element_scaled_minus_element") >= 67, "PREMIUM").otherwise("STANDARD"))
    .cube("currency_stddev_samp_element_tier", "element_scaled_minus_element_tier")
    .agg(
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.percentile_approx("element_dense_rank", [0.25, 0.5, 0.75]).alias("element_dense_rank_quartiles"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.percentile_approx("element_scaled_minus_element_scaled", 0.95).alias("element_scaled_minus_element_scaled_p95"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("element_scaled_minus_element_scaled_p95") < 1000000)
)

result = (
    daily
    .select(
        "currency_stddev_samp_element_tier",
        "element_scaled_minus_element_tier",
        "avg_element",
        "element_dense_rank_quartiles",
        "element_scaled_minus_element_scaled_p95",
        "grouping_id",
    )
    .orderBy(F.col("grouping_id").desc())
)
