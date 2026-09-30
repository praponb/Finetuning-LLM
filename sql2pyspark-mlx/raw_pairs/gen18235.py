from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on_plus_months", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("promo_code")
w3 = Window.partitionBy("promo_code").orderBy("last3_sum_days_since_ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy(F.col("last3_count_days_since_ends_on").desc(), "id")
w5 = Window.partitionBy("promo_code").orderBy(F.col("last3_count_days_since_ends_on_bucket").desc_nulls_last())
w_part = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("first_known_last3_sum_days_since_ends_on", "id")
w8 = Window.partitionBy("promo_code").orderBy(F.col("promo_code_count_last3_max_days_since_ends_on").desc(), "id")

base = (
    df_promotions
    .filter(F.col("starts_on").between("2023-12-05", "2025-01-01"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.lower("promo_code").alias("promo_code_lower"),
        F.add_months("ends_on", 6).alias("ends_on_plus_months"),
    )
)

filtered = (
    base
    .filter((F.col("ends_on") >= F.lit("2023-09-10").cast("date")) & (F.col("ends_on_plus_months") <= "2023-08-23"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_code_lower",
        "ends_on_plus_months",
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

enriched = (
    filtered
    .filter(F.col("starts_on") >= "2024-03-01")
    .select(
        "id",
        "promo_code",
        "ends_on",
        F.count("days_since_ends_on").over(w).alias("last3_count_days_since_ends_on"),
        F.max("days_since_ends_on").over(w).alias("last3_max_days_since_ends_on"),
        F.sum("days_since_ends_on").over(w).alias("last3_sum_days_since_ends_on"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.count("last3_max_days_since_ends_on").over(w2).alias("promo_code_count_last3_max_days_since_ends_on"),
        F.first("last3_sum_days_since_ends_on", ignorenulls=True).over(w3).alias("first_known_last3_sum_days_since_ends_on"),
        F.ntile(10).over(w4).alias("last3_count_days_since_ends_on_bucket"),
    )
)

enriched_promotions = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w5).alias("last3_count_days_since_ends_on_bucket_percent_rank"),
        F.sum("last3_max_days_since_ends_on").over(w_part).alias("cumulative_last3_max_days_since_ends_on"),
        F.ntile(10).over(w7).alias("first_known_last3_sum_days_since_ends_on_bucket"),
    )
)

deduped = (
    enriched_promotions
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .cube(
        F.when(F.col("last3_count_days_since_ends_on") >= 16, "HIGH").otherwise("LOW").alias("last3_count_days_since_ends_on_tier"),
        "promo_code",
        F.when(F.col("last3_sum_days_since_ends_on") >= 340, "PREMIUM").otherwise("STANDARD").alias("last3_sum_days_since_ends_on_tier"),
    )
    .agg(
        F.sum(F.when(F.col("promo_code") == "VIP", F.col("last3_sum_days_since_ends_on"))).alias("vip_total_last3_sum_days_since_ends_on"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.percentile_approx("last3_sum_days_since_ends_on", 0.5).alias("last3_sum_days_since_ends_on_p50"),
        F.avg(F.when(F.col("promo_code") == "WELCOME5", F.col("last3_count_days_since_ends_on"))).alias("welcome5_avg_last3_count_days_since_ends_on"),
        F.avg(F.when(F.col("promo_code") == "SPRING10", F.col("promo_code_count_last3_max_days_since_ends_on"))).alias("spring10_avg_promo_code_count_last3_max_days_since_ends_on"),
        F.sum(F.when(F.col("promo_code") == "SPRING10", F.col("last3_max_days_since_ends_on")).otherwise(0)).alias("spring10_last3_max_days_since_ends_on"),
    )
    .orderBy(F.col("promo_code").desc())
)
