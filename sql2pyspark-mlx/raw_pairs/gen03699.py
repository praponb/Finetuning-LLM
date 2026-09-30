from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")
w2 = Window.orderBy("amount", "id")
w3 = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_first())
w4 = Window.partitionBy(F.month("paid_at"), "status").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("status")
w6 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy(F.month("paid_at"), "order_id")
w8 = Window.partitionBy("order_id").orderBy(F.col("amount_bucket").desc_nulls_first())
w9 = Window.partitionBy("last3_min_amount_bucket_tier").orderBy(F.col("paypal_count").desc())

scored = (
    df_payments
    .select(
        "*",
        F.lead("amount", 2).over(w).alias("next_amount"),
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.rank().over(w3).alias("amount_rank"),
        F.avg("amount").over(w4).alias("amount_avg_2"),
        F.round((F.col("amount") / F.sum("amount").over(w5)), 4).alias("amount_share"),
    )
)

base = (
    scored
    .filter(~F.col("method").eqNullSafe(F.lit("card")))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "next_amount",
        "amount_bucket",
        "amount_rank",
        F.year("paid_at").alias("paid_at_year"),
    )
)

enriched = (
    base
    .select(
        "id",
        "order_id",
        "method",
        "amount_bucket",
        "amount_rank",
        F.count("amount_bucket").over(w6).alias("last3_count_amount_bucket"),
        F.max("amount_bucket").over(w6).alias("last3_max_amount_bucket"),
        F.min("amount_bucket").over(w6).alias("last3_min_amount_bucket"),
        F.stddev_samp("next_amount").over(w7).alias("paid_at_month_stddev_samp_next_amount"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.rank().over(w8))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "method",
        "amount_bucket",
        "amount_rank",
        "last3_count_amount_bucket",
        "last3_min_amount_bucket",
        "paid_at_month_stddev_samp_next_amount",
    )
)

grouped = (
    deduped
    .groupBy(
        F.when(F.col("last3_min_amount_bucket") >= 8.9, "PREMIUM").otherwise("STANDARD").alias("last3_min_amount_bucket_tier"),
        "method",
        F.when(F.col("last3_count_amount_bucket") >= 13, "LARGE").otherwise("SMALL").alias("last3_count_amount_bucket_tier"),
    )
    .agg(
        F.percentile_approx("last3_count_amount_bucket", 0.95).alias("last3_count_amount_bucket_p95"),
        F.percentile_approx("amount_rank", 0.9).alias("amount_rank_p90"),
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
        F.percentile_approx("last3_min_amount_bucket", [0.25, 0.5, 0.75]).alias("last3_min_amount_bucket_quartiles"),
        F.avg("last3_min_amount_bucket").alias("avg_last3_min_amount_bucket"),
    )
    .withColumn("paypal_count_rank", F.rank().over(w9))
)

result = (
    grouped
    .select(
        "last3_min_amount_bucket_tier",
        "method",
        "last3_count_amount_bucket_tier",
        "last3_count_amount_bucket_p95",
        "amount_rank_p90",
        "paypal_count",
        "last3_min_amount_bucket_quartiles",
        "avg_last3_min_amount_bucket",
        "paypal_count_rank",
    )
    .orderBy(F.col("method").desc())
)
