from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "ends_on"), "channel").orderBy(F.col("discount_pct").desc())
w2 = Window.partitionBy(F.month("starts_on"), "channel")
w3 = Window.orderBy(F.col("discount_pct_cume_dist").desc(), "id")
w4 = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id")
w5 = Window.partitionBy("channel").orderBy("starts_on_month_max_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w7 = Window.partitionBy(F.date_trunc("MONTH", "ends_on"), "promo_code").orderBy(F.col("starts_on_month_avg_discount_pct").desc(), "id")
w8 = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w_ordered = Window.orderBy(F.col("cumulative_discount_pct").desc(), "id")
w_running = Window.partitionBy("promo_code").orderBy(F.col("event_seq").desc(), "id")

cleaned = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-04-15").cast("date"))
    .select(
        "*",
        F.initcap(F.trim("channel")).alias("channel_title"),
        F.when(F.col("channel").isin(["mobile", "web"]), "Core").otherwise("Other").alias("channel_group"),
    )
)

scored = (
    cleaned
    .filter(F.col("starts_on") <= "2024-04-01")
    .select(
        "*",
        F.cume_dist().over(w).alias("discount_pct_cume_dist"),
        F.avg("discount_pct").over(w2).alias("starts_on_month_avg_discount_pct"),
        F.max("discount_pct").over(w2).alias("starts_on_month_max_discount_pct"),
    )
)

scored_promotions = (
    scored
    .select(
        "*",
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.sum("discount_pct").over(w5).alias("cumulative_discount_pct"),
        F.row_number().over(w6).alias("event_seq"),
        F.row_number().over(w7).alias("row_seq_3"),
    )
)

deduped = (
    scored_promotions
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "channel_title",
        "channel_group",
        "discount_pct_cume_dist",
        "row_seq_2",
        "row_seq_3",
        "latest_rank",
        F.ntile(3).over(w_ordered).alias("cumulative_discount_pct_bucket"),
        F.ntile(10).over(w_running).alias("event_seq_bucket"),
    )
)

result = (
    metrics
    .withColumn("row_seq_2_tier", F.when(F.col("row_seq_2") >= 6, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["promo_code", "row_seq_2_tier"], ["promo_code"], []],
        "promo_code",
        "row_seq_2_tier",
    )
    .agg(
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_count"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "VIP", 1)), F.lit(0)), 2).alias("discount_pct_per_vip"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.grouping("promo_code").alias("grouping_id"),
    )
    .filter(F.col("discount_pct_per_vip") >= 0)
)
