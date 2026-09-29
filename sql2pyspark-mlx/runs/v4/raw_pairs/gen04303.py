from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("val"),
    )
)

cleaned = (
    exploded
    .filter(F.col("effective_from") >= F.lit("2024-08-27").cast("date"))
    .withColumn("val_tier", F.when(F.col("val") >= 73, "High").when(F.col("val") >= 37, "Medium").otherwise("Low"))
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("currency") == "USD")
    .select(
        "id",
        "effective_from",
        "currency",
        "val",
        F.add_months("effective_from", 6).alias("effective_from_plus_months"),
    )
)

enriched = (
    cleaned_price_tiers
    .select(
        "*",
        F.cume_dist().over(w).alias("val_cume_dist"),
        F.rank().over(w2).alias("val_rank"),
        F.stddev_samp("val").over(w3).alias("currency_stddev_samp_val"),
    )
)

summary = (
    enriched
    .groupBy(
        "currency",
        F.when(F.col("currency_stddev_samp_val") >= 62, "HIGH").otherwise("LOW").alias("currency_stddev_samp_val_tier"),
        F.when(F.col("val") >= 74, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
    )
    .agg(
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
        F.sum(F.when(F.col("currency") == "THB", F.col("val")).otherwise(0)).alias("thb_val"),
        F.sum(F.when(F.col("currency") == "THB", F.col("val")).otherwise(0)).alias("thb_val_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
        F.avg("val_cume_dist").alias("avg_val_cume_dist"),
    )
)

result = (
    summary
    .select(
        "currency",
        "currency_stddev_samp_val_tier",
        "val_tier",
        "thb_events",
        "thb_val_2",
        "avg_val_cume_dist",
        F.when(F.col("currency").rlike("^(EUR|THB)$"), "EXTENDED").when(F.col("currency").rlike("^(USD)$"), "PRIORITY").otherwise("CORE").alias("currency_zone"),
    )
    .orderBy(F.col("currency_stddev_samp_val_tier").desc_nulls_last())
)
