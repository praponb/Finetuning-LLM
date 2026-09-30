from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("days_since_ends_on").asc_nulls_first())
w_running = Window.partitionBy("channel")
w3 = Window.partitionBy("channel").orderBy("adjusted_discount_pct", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("channel", "promo_code").orderBy(F.col("days_since_ends_on_rank").desc_nulls_last(), "id")

base = (
    df_promotions
    .filter(F.col("starts_on") >= "2024-03-07")
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
        F.round((F.col("discount_pct") * 0.85), 2).alias("adjusted_discount_pct"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

scored = (
    base
    .select(
        "*",
        F.rank().over(w).alias("days_since_ends_on_rank"),
        F.count("adjusted_discount_pct").over(w_running).alias("channel_count_adjusted_discount_pct"),
        F.avg("days_since_ends_on").over(w3).alias("days_since_ends_on_avg_2_prior"),
    )
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy(
        "channel",
        F.when(F.col("days_since_ends_on") >= 69, "LARGE").otherwise("SMALL").alias("days_since_ends_on_tier"),
    )
    .agg(
        F.round(F.avg("days_since_ends_on_rank"), 2).alias("avg_days_since_ends_on_rank"),
        F.count("*").alias("event_count"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("days_since_ends_on_avg_2_prior", [0.25, 0.5, 0.75]).alias("days_since_ends_on_avg_2_prior_quartiles"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
    .orderBy(F.col("avg_days_since_ends_on_rank").desc())
)
