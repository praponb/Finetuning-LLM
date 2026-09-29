from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("discount_pct").desc(), "id")
w3 = Window.partitionBy("channel").orderBy("discount_pct")
w4 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy(F.month("ends_on")).orderBy(F.col("discount_pct").desc(), "id")
w6 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct_bucket").desc())
w7 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("channel").orderBy("running_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("promo_code").orderBy(F.col("running_sum_discount_pct_bucket").asc_nulls_first())

metrics = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        F.sum("discount_pct").over(w).alias("running_discount_pct"),
        F.row_number().over(w2).alias("row_seq"),
        F.rank().over(w3).alias("discount_pct_rank"),
        F.avg("discount_pct").over(w4).alias("discount_pct_avg_5"),
        F.ntile(3).over(w5).alias("discount_pct_bucket"),
    )
)

latest = (
    metrics
    .withColumn("rn", F.rank().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics_promotions = (
    latest
    .filter(F.col("discount_pct").between(0.13, 0.19))
    .select(
        "id",
        "promo_code",
        "discount_pct",
        "discount_pct_rank",
        F.max("discount_pct_bucket").over(w7).alias("running_max_discount_pct_bucket"),
        F.sum("discount_pct_bucket").over(w7).alias("running_sum_discount_pct_bucket"),
        F.avg("discount_pct_bucket").over(w7).alias("running_avg_discount_pct_bucket"),
        F.sum("row_seq").over(w8).alias("running_row_seq"),
    )
)

cleaned = (
    metrics_promotions
    .filter(F.col("running_sum_discount_pct_bucket") > 34)
    .withColumn("discount_pct_level", F.when(F.col("discount_pct") >= 0.35, "High").when(F.col("discount_pct") >= 0.12, "Medium").otherwise("Low"))
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w9))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .withColumn("running_row_seq_tier", F.when(F.col("running_row_seq") >= 45, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["running_row_seq_tier", "promo_code"], ["running_row_seq_tier"]],
        "running_row_seq_tier",
        "promo_code",
    )
    .agg(
        F.round(F.avg("discount_pct_rank"), 2).alias("avg_discount_pct_rank"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
    )
    .orderBy(F.col("running_row_seq_tier").asc_nulls_first())
)
