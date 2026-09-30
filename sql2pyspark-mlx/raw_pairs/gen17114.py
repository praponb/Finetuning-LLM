from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(-2, -1)
w3 = Window.orderBy(F.col("starts_on").desc(), "id")
w4 = Window.partitionBy(F.month("starts_on"), "channel").orderBy("discount_pct_whole", "id")
w5 = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy(F.col("seq_no").desc_nulls_last(), "id")
w_part = Window.partitionBy("promo_code", "channel").orderBy("discount_pct_whole", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("discount_pct") >= 0.14)
    .select("*", F.floor("discount_pct").alias("discount_pct_whole"))
)

scored = (
    cleaned
    .filter(F.col("discount_pct_whole").between(40, 42))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4))
    .withColumn("discount_pct_avg_2_prior", F.avg("discount_pct").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1).over(w4))
    .withColumn("running_discount_pct_whole", F.sum("discount_pct_whole").over(w5))
)

latest = (
    scored
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    latest
    .filter(F.col("discount_pct_share") < 0.73)
    .withColumn("ends_on_plus_37d", F.date_add("ends_on", 37))
    .withColumn("latest_rank_whole", F.floor("latest_rank"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("OTHER")))
)

metrics = (
    filtered
    .withColumn("last4_min_discount_pct_share", F.min("discount_pct_share").over(w_part))
    .withColumn("last4_avg_discount_pct_share", F.avg("discount_pct_share").over(w_part))
    .withColumn("last4_max_discount_pct_share", F.max("discount_pct_share").over(w_part))
)

result = (
    metrics
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_avg_2_prior",
        "seq_no",
        "prev_discount_pct",
    )
)
