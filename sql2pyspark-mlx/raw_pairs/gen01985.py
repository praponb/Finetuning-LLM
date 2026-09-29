from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("ends_on", "id")
w_ordered = Window.partitionBy("promo_code").orderBy("last4_avg_discount_pct", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy(F.col("last4_count_discount_pct").asc_nulls_last())
w5 = Window.orderBy("next_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.month("ends_on"), "promo_code").orderBy(F.col("last4_avg_next_discount_pct").desc())
w7 = Window.partitionBy("promo_code").orderBy("last4_count_next_discount_pct", "id")
w8 = Window.partitionBy("promo_code")

valid = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-08-01").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.datediff("ends_on", "starts_on").alias("days_starts_on_to_ends_on"),
    )
)

enriched = (
    valid
    .filter(F.month("ends_on").isin([3, 5, 8]))
    .withColumn("last4_sum_discount_pct", F.sum("discount_pct").over(w))
    .withColumn("last4_avg_discount_pct", F.avg("discount_pct").over(w))
    .withColumn("last4_count_discount_pct", F.count("discount_pct").over(w))
    .withColumn("last4_min_discount_pct", F.min("discount_pct").over(w))
    .withColumn("next_discount_pct", F.lead("discount_pct", 2, 0).over(w2))
)

metrics = (
    enriched
    .withColumn("last4_count_next_discount_pct", F.count("next_discount_pct").over(w_ordered))
    .withColumn("last4_avg_next_discount_pct", F.avg("next_discount_pct").over(w_ordered))
    .withColumn("last4_min_next_discount_pct", F.min("next_discount_pct").over(w_ordered))
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "last4_count_discount_pct",
        "last4_min_discount_pct",
        "next_discount_pct",
        "last4_count_next_discount_pct",
        "last4_avg_next_discount_pct",
    )
)

metrics_promotions = (
    deduped
    .select(
        "*",
        F.last("last4_count_next_discount_pct", ignorenulls=True).over(w5).alias("last_known_last4_count_next_discount_pct"),
        F.cume_dist().over(w6).alias("last4_avg_next_discount_pct_cume_dist"),
        F.lag("last4_min_discount_pct", 2).over(w7).alias("prev_last4_min_discount_pct"),
        F.min("last4_min_discount_pct").over(w8).alias("promo_code_min_last4_min_discount_pct"),
    )
)

result = (
    metrics_promotions
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "next_discount_pct",
        "last4_count_next_discount_pct",
        "last4_avg_next_discount_pct",
        "last4_avg_next_discount_pct_cume_dist",
        "prev_last4_min_discount_pct",
    )
)
