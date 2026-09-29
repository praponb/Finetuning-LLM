from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("ends_on_plus_13d").desc(), "id")
w2 = Window.partitionBy(F.month("ends_on_plus_13d"), "promo_code")
w3 = Window.partitionBy("promo_code")
w4 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w5 = Window.partitionBy("promo_code").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.month("starts_on"), "promo_code").orderBy("discount_pct", "id")
w_part = Window.orderBy("running_discount_pct", "id")
w8 = Window.orderBy("discount_pct_share_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("running_discount_pct")

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.lower("promo_code").alias("promo_code_lower"),
        F.date_add("ends_on", 13).alias("ends_on_plus_13d"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    ranked
    .select(
        "*",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4).alias("discount_pct_share_2"),
        F.lead("discount_pct", 2).over(w4).alias("next_discount_pct"),
        F.ntile(10).over(w4).alias("discount_pct_bucket"),
    )
)

ranked_promotions = (
    scored
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    ranked_promotions
    .select(
        "id",
        "discount_pct",
        "promo_code_lower",
        "ends_on_plus_13d",
        "channel_clean",
        "discount_pct_share_2",
        "next_discount_pct",
        "discount_pct_bucket",
        F.sum("discount_pct").over(w5).alias("running_discount_pct"),
        F.ntile(3).over(w6).alias("discount_pct_bucket_2"),
    )
)

metrics_promotions = (
    metrics
    .select(
        "*",
        F.ntile(10).over(w_part).alias("running_discount_pct_bucket"),
        F.first("next_discount_pct", ignorenulls=True).over(w8).alias("first_known_next_discount_pct"),
        F.dense_rank().over(w9).alias("running_discount_pct_dense_rank"),
    )
)

filtered = (
    metrics_promotions
    .filter(F.col("discount_pct_share_2") > 0.37)
    .select(
        "*",
        F.greatest("first_known_next_discount_pct", "running_discount_pct_dense_rank").alias("greatest_first_known_next_discount_pct_running_discount_pct_dense_rank"),
    )
)

result = (
    filtered
    .cube(
        F.when(F.col("discount_pct_bucket_2") >= 8, "HIGH").otherwise("LOW").alias("discount_pct_bucket_2_tier"),
        F.date_trunc("QUARTER", "ends_on_plus_13d").alias("quarter_start"),
        F.date_trunc("WEEK", "ends_on_plus_13d").alias("week_start"),
    )
    .agg(
        F.sum("discount_pct_bucket").alias("total_discount_pct_bucket"),
        F.avg("greatest_first_known_next_discount_pct_running_discount_pct_dense_rank").alias("avg_greatest_first_known_next_discount_pct_running_discount_pct_dense_rank"),
        F.sum("running_discount_pct_dense_rank").alias("total_running_discount_pct_dense_rank"),
    )
    .filter(F.col("avg_greatest_first_known_next_discount_pct_running_discount_pct_dense_rank") > 0)
    .orderBy(F.col("week_start").desc())
)
