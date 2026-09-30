from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("is_amazon", "id")
w3 = Window.partitionBy("merchant")
w4 = Window.partitionBy("account_id").orderBy("running_is_amazon", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant", "account_id").orderBy("is_amazon_bucket", "id")
w6 = Window.orderBy("is_amazon_bucket", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.orderBy("running_avg_is_amazon_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter((~F.col("merchant").eqNullSafe(F.lit("Grab"))) & F.col("account_id").isin([2, 13, 40]))
    .select(
        "*",
        F.when(F.col("merchant") == "Amazon", 1).otherwise(0).alias("is_amazon"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "account_id",
        "merchant",
        "is_flagged",
        F.sum("is_amazon").over(w).alias("running_is_amazon"),
        F.ntile(3).over(w2).alias("is_amazon_bucket"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "running_is_amazon",
        "is_amazon_bucket",
        F.sum("is_amazon_bucket").over(w4).alias("running_sum_is_amazon_bucket"),
        F.avg("is_amazon_bucket").over(w4).alias("running_avg_is_amazon_bucket"),
        F.max("is_amazon_bucket").over(w4).alias("running_max_is_amazon_bucket"),
        F.lead("running_is_amazon", 1).over(w5).alias("next_running_is_amazon"),
    )
)

filtered = (
    metrics
    .filter(F.col("running_avg_is_amazon_bucket") > 2.9)
    .select(
        "id",
        "running_is_amazon",
        "is_amazon_bucket",
        "running_sum_is_amazon_bucket",
        "running_avg_is_amazon_bucket",
        "running_max_is_amazon_bucket",
        "next_running_is_amazon",
        F.round("running_avg_is_amazon_bucket", 2).alias("running_avg_is_amazon_bucket_rounded"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "is_amazon_bucket",
        "running_sum_is_amazon_bucket",
        "running_max_is_amazon_bucket",
        F.avg("next_running_is_amazon").over(w6).alias("next_running_is_amazon_avg_2"),
        F.last("running_avg_is_amazon_bucket_rounded", ignorenulls=True).over(w7).alias("last_known_running_avg_is_amazon_bucket_rounded"),
    )
)

base = (
    scored
    .filter(F.col("next_running_is_amazon_avg_2") > 112)
    .withColumn("running_max_is_amazon_bucket_adjusted", F.round((F.col("running_max_is_amazon_bucket") * 1.1), 2))
)

cleaned_transactions = (
    base
    .filter(F.col("running_sum_is_amazon_bucket").between(32, 37))
    .withColumn("running_sum_is_amazon_bucket_plus_last_known_running_avg_is_amazon_bucket_rounded", (F.col("running_sum_is_amazon_bucket") + F.col("last_known_running_avg_is_amazon_bucket_rounded")))
    .withColumn("next_running_is_amazon_avg_2_whole", F.ceil("next_running_is_amazon_avg_2"))
)

result = (
    cleaned_transactions
    .groupBy(
        F.when(F.col("next_running_is_amazon_avg_2_whole") >= 68, "PREMIUM").otherwise("STANDARD").alias("next_running_is_amazon_avg_2_whole_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("last_known_running_avg_is_amazon_bucket_rounded"), 2).alias("avg_last_known_running_avg_is_amazon_bucket_rounded"),
    )
    .filter(F.col("row_count") >= 1)
    .orderBy(F.col("avg_last_known_running_avg_is_amazon_bucket_rounded").desc())
)
