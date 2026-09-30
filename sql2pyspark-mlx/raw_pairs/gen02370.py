from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("promotion_promo_code", "promo_code").orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("promo_code").orderBy("starts_on", "id", "promo_id")
w_running = Window.orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_id_tier").orderBy(F.col("cnt").desc())

matched = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows = (
    matched
    .filter(F.year("starts_on") == 2024)
    .withColumn("running_min_discount_pct", F.min("discount_pct").over(w_part))
    .withColumn("running_avg_discount_pct", F.avg("discount_pct").over(w_part))
    .withColumn("running_max_discount_pct", F.max("discount_pct").over(w_part))
)

enriched = (
    with_windows
    .select(
        "id",
        "starts_on",
        "promo_id",
        "running_avg_discount_pct",
        "running_max_discount_pct",
        F.lag("discount_pct").over(w_grp).alias("prev_discount_pct"),
        F.lead("running_min_discount_pct").over(w_grp).alias("next_running_min_discount_pct"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.first("running_min_discount_pct", ignorenulls=True).over(w_running).alias("first_known_running_min_discount_pct"),
    )
)

cleaned = (
    enriched
    .filter(F.col("starts_on") >= F.lit("2023-08-26").cast("date"))
    .select(
        "id",
        "starts_on",
        "promo_id",
        "running_avg_discount_pct",
        "running_max_discount_pct",
        "row_seq",
        "first_known_running_min_discount_pct",
        F.round((F.col("running_max_discount_pct") * 0.85), 2).alias("running_max_discount_pct_scaled"),
    )
)

cleaned_promotions = (
    cleaned
    .filter(F.col("running_max_discount_pct_scaled").between(23, 47))
    .select(
        "id",
        "starts_on",
        "promo_id",
        "running_max_discount_pct",
        "running_max_discount_pct_scaled",
        F.coalesce("first_known_running_min_discount_pct", F.lit(0)).alias("first_known_running_min_discount_pct_filled"),
        (F.col("running_max_discount_pct_scaled") - F.col("running_avg_discount_pct")).alias("running_max_discount_pct_scaled_minus_running_avg_discount_pct"),
    )
)

result = (
    cleaned_promotions
    .groupBy(
        F.when(F.col("promo_id") >= 8, "HIGH").otherwise("LOW").alias("promo_id_tier"),
        F.when(F.col("running_max_discount_pct_scaled") >= 63, "PREMIUM").otherwise("STANDARD").alias("running_max_discount_pct_scaled_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg("first_known_running_min_discount_pct_filled").alias("avg_first_known_running_min_discount_pct_filled"),
        F.percentile_approx("promo_id", [0.25, 0.5, 0.75]).alias("promo_id_quartiles"),
    )
    .withColumn("cnt_percent_rank", F.percent_rank().over(w4))
    .orderBy(F.col("running_max_discount_pct_scaled_tier").desc_nulls_last())
)
