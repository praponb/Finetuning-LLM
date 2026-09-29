from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy(F.month("ends_on")).orderBy("channel_length", "id")
w3 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("ends_on", "id")
w6 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter(F.year("starts_on") == 2025)
    .withColumn("is_web", F.when(F.col("channel") == "web", 1).otherwise(0))
    .withColumn("channel_length", F.length("channel"))
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "channel_length",
        "latest_rank",
        F.ntile(3).over(w2).alias("channel_length_bucket"),
        F.last("is_web", ignorenulls=True).over(w3).alias("last_known_is_web"),
        F.sum("discount_pct").over(w4).alias("running_discount_pct"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "ends_on",
        "latest_rank",
        "running_discount_pct",
        F.lead("running_discount_pct", 1).over(w5).alias("next_running_discount_pct"),
        F.sum("channel_length_bucket").over(w6).alias("cumulative_channel_length_bucket"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("cumulative_channel_length_bucket") >= 42, "LARGE").otherwise("SMALL").alias("cumulative_channel_length_bucket_tier"),
        F.when(F.col("next_running_discount_pct") >= 1, "PREMIUM").otherwise("STANDARD").alias("next_running_discount_pct_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("running_discount_pct", [0.25, 0.5, 0.75]).alias("running_discount_pct_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("next_running_discount_pct_tier").desc_nulls_last())
)
