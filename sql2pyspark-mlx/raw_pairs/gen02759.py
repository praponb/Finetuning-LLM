from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct_scaled", "id")
w2 = Window.orderBy(F.col("discount_pct_scaled").desc(), "id")
w3 = Window.partitionBy(F.month("ends_on")).orderBy("starts_on", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy(F.year("ends_on")).orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("promo_code").orderBy("running_discount_pct_scaled")
w7 = Window.partitionBy("promo_code").orderBy("cumulative_discount_pct_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("running_discount_pct_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct_scaled_avg_2", "id")
w10 = Window.partitionBy("channel")

base = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.trim("promo_code").alias("clean_promo_code"),
        F.round((F.col("discount_pct") * 0.85), 2).alias("discount_pct_scaled"),
    )
)

scored = (
    base
    .filter(F.col("channel") == "partner")
    .select(
        "id",
        "channel",
        "promo_code",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(10).over(w2).alias("discount_pct_scaled_bucket"),
        F.avg("discount_pct_scaled").over(w3).alias("discount_pct_scaled_avg_2"),
        F.sum("discount_pct_scaled").over(w4).alias("running_discount_pct_scaled"),
        F.sum("discount_pct_scaled").over(w5).alias("cumulative_discount_pct_scaled"),
    )
)

metrics = (
    scored
    .withColumn("running_discount_pct_scaled_cume_dist", F.cume_dist().over(w6))
    .withColumn("cumulative_discount_pct_scaled_avg_2", F.sum("discount_pct_scaled_avg_2").over(w7))
    .withColumn("running_running_discount_pct_scaled", F.sum("running_discount_pct_scaled").over(w8))
)

scored_promotions = (
    metrics
    .select(
        "id",
        "channel",
        "promo_code",
        "row_seq",
        "cumulative_discount_pct_scaled_avg_2",
        F.row_number().over(w9).alias("row_seq_2"),
        F.max("discount_pct_scaled_avg_2").over(w10).alias("channel_max_discount_pct_scaled_avg_2"),
    )
)

result = (
    scored_promotions
    .rollup(
        F.when(F.col("channel_max_discount_pct_scaled_avg_2") >= 73, "LARGE").otherwise("SMALL").alias("channel_max_discount_pct_scaled_avg_2_tier"),
    )
    .agg(
        F.avg(F.when(F.col("promo_code") == "BUNDLE", F.col("row_seq_2"))).alias("bundle_avg_row_seq_2"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
    )
    .orderBy(F.col("partner_rows").asc_nulls_last())
)
