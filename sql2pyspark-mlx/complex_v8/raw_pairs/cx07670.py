from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_from_to_date_tariff_by = Window.partitionBy(F.to_date("valid_from"), "tariff").orderBy("rate", "id")
w_valid_from_month_tariff_by_r = Window.partitionBy(F.date_trunc("month", "valid_from"), "tariff").orderBy("rate_distance", "id").rowsBetween(-6, -1)
w_tariff = Window.partitionBy("tariff")
w_valid_from_month_tariff_by_r_2 = Window.partitionBy(F.date_trunc("month", "valid_from"), "tariff").orderBy("rate_distance")
w_rate_quartile = Window.partitionBy("rate_quartile")
w_tariff_rate_distance_rank_by = Window.partitionBy("tariff", "rate_distance_rank").orderBy("valid_from", "id")
w_dedup = Window.partitionBy("tariff").orderBy(F.col("rate_quartile").desc(), F.col("id").desc())

filtered = (
    df_tariff_windows
    .filter(F.col("tariff").isin("TOU", "FLAT"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.abs(F.col("rate") - 4.2).alias("rate_distance"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("valid_to")).alias("days_since_valid_to"),
    )
)

ranked = (
    filtered
    .select(
        "*",
        F.ntile(4).over(w_valid_from_to_date_tariff_by).alias("rate_quartile"),
        F.max("rate").over(w_valid_from_month_tariff_by_r).alias("rate_max_6_prior"),
        F.max("rate").over(w_tariff).alias("tariff_max_rate"),
        F.row_number().over(w_valid_from_to_date_tariff_by).alias("seq_no"),
        F.rank().over(w_valid_from_month_tariff_by_r_2).alias("rate_distance_rank"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.min("days_since_valid_to").over(w_rate_quartile).alias("rate_quartile_min_days_since_valid_to"),
        F.size(F.collect_set("tariff").over(w_rate_quartile)).alias("distinct_tariff_in_rate_quartile"),
        F.lead("valid_from", 3).over(w_tariff_rate_distance_rank_by).alias("next3_valid_from"),
        F.lag("rate_quartile").over(w_tariff_rate_distance_rank_by).alias("prev_rate_quartile"),
        F.round(F.col("rate_max_6_prior") - F.avg("rate_max_6_prior").over(w_rate_quartile), 2).alias("rate_max_6_prior_vs_rate_quartile_avg"),
        F.round(F.col("days_since_valid_to") / F.nullif(F.sum("days_since_valid_to").over(w_rate_quartile), F.lit(0)), 4).alias("days_since_valid_to_share_of_rate_quartile"),
    )
)

annotated = (
    metrics
    .select(
        "*",
        F.round(F.col("tariff_max_rate") / F.nullif(F.col("seq_no"), F.lit(0)), 3).alias("tariff_max_rate_per_seq_no"),
        F.substring(F.trim(F.col("tariff")), 1, 4).alias("tariff_prefix"),
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
    )
)

latest_per_key = (
    annotated
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enriched = (
    latest_per_key
    .select(
        "*",
        F.round(F.col("rate") / F.nullif(F.col("tariff_len"), F.lit(0)), 3).alias("rate_per_tariff_len"),
        F.round(F.col("seq_no") / F.nullif(F.col("rate_distance"), F.lit(0)), 3).alias("seq_no_per_rate_distance"),
        F.when(F.col("tariff").isin("EV", "TOU"), "group_a").when(F.col("tariff") == "FLAT", "group_b").otherwise("other").alias("tariff_group"),
    )
)

result = (
    enriched
    .filter(F.col("days_since_valid_to_share_of_rate_quartile") < 0.7)
    .select(
        "id",
        "valid_from",
        "rate",
        "rate_distance",
        "days_since_valid_to",
        "rate_max_6_prior",
        "rate_distance_rank",
        "rate_quartile_min_days_since_valid_to",
        "next3_valid_from",
        "rate_max_6_prior_vs_rate_quartile_avg",
        "days_since_valid_to_share_of_rate_quartile",
        "tariff_max_rate_per_seq_no",
        "seq_no_per_rate_distance",
        "tariff_group",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("next3_valid_from")).alias("days_since_next3_valid_from"),
        F.round((F.unix_timestamp(F.col("next3_valid_from")) - F.unix_timestamp(F.col("valid_from"))) / 60, 1).alias("minutes_valid_from_to_next3_valid_from"),
    )
    .orderBy(F.col("rate_distance").desc())
)
