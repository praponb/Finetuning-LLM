from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("starts_on")).orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel")
w_hist = Window.partitionBy(F.date_trunc("month", "ends_on"), "channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("ends_on", "id")
w6 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-2, -1)
w8 = Window.orderBy(F.col("running_discount_pct").asc_nulls_last())

metrics = (
    df_promotions
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w))
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w2))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w_hist))
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    latest
    .filter(F.col("starts_on") >= F.to_date(F.lit("2023-04-15")))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "first_known_channel",
        "running_discount_pct",
        "last_known_channel",
        "dedup_rank",
        F.substring("channel", 1, 3).alias("channel_prefix"),
    )
)

metrics_promotions = (
    filtered
    .filter(F.col("running_discount_pct") >= 1)
    .select(
        "*",
        F.first("running_discount_pct", ignorenulls=True).over(w6).alias("first_known_running_discount_pct"),
        F.avg("running_discount_pct").over(w7).alias("running_discount_pct_avg_2_prior"),
        F.rank().over(w8).alias("running_discount_pct_rank"),
    )
)

result = (
    metrics_promotions
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "first_known_channel",
        "running_discount_pct",
        "last_known_channel",
        "dedup_rank",
        "channel_prefix",
        F.when(F.col("promo_code").rlike("^(BUNDLE|SPRING10)$"), "GROUP_B").when(F.col("promo_code").rlike("^(VIP|WELCOME5)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("promo_code_zone"),
    )
    .orderBy(F.col("id").desc(), F.col("ends_on").desc())
)
