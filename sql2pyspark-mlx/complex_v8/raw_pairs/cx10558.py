from pyspark.sql import functions as F

filtered = (
    df_tariff_windows
    .filter(F.col("tariff") == "TOU")
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.round(F.col("rate") * 0.9, 2).alias("rate_adj"),
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
    )
)

stats = (
    filtered
    .filter(F.hour(F.col("valid_from")).between(6, 12))
    .groupBy(F.year("valid_from").alias("valid_from_year"), "tariff")
    .agg(
        F.max("valid_from").alias("last_valid_from"),
        F.round(F.sum("rate_adj"), 2).alias("total_rate_adj"),
        F.sum(F.when(F.col("tariff").isNotNull(), 1).otherwise(0)).alias("n_tariff_is_not_null"),
        F.array_sort(F.collect_set("tariff")).alias("tariff_list"),
        F.sum(F.when(F.col("tariff") == "FLAT", 1).otherwise(0)).alias("n_tariff_flat"),
        F.sum(F.when(F.col("tariff") != "EV", F.col("rate"))).alias("rate_tariff_ev"),
        F.avg("rate").alias("avg_rate_check"),
    )
    .filter(F.col("avg_rate_check") > 4.2)
    .drop("avg_rate_check")
)

enriched = (
    stats
    .select(
        "*",
        F.least(F.col("n_tariff_is_not_null"), F.col("rate_tariff_ev")).alias("least_n_tariff_is_not_null_rate_tariff_ev"),
        (F.col("total_rate_adj") - F.col("n_tariff_is_not_null")).alias("total_rate_adj_minus_n_tariff_is_not_null"),
    )
)

enriched_2 = (
    enriched
    .filter(F.col("rate_tariff_ev") >= 14.0)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
    )
)

result = (
    enriched_2
    .select(
        "valid_from_year",
        "tariff",
        "last_valid_from",
        "total_rate_adj",
        "n_tariff_is_not_null",
        "tariff_list",
        "rate_tariff_ev",
        "least_n_tariff_is_not_null_rate_tariff_ev",
        "total_rate_adj_minus_n_tariff_is_not_null",
    )
    .orderBy("tariff", "valid_from_year")
    .limit(5)
)
