from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("adjusted_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("channel")
w4 = Window.partitionBy("promo_code").orderBy(F.col("adjusted_discount_pct").desc_nulls_first())
w5 = Window.partitionBy("promo_code", "channel").orderBy(F.col("cumulative_adjusted_discount_pct").desc_nulls_first(), "id")

valid = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-05-16").cast("date"))
    .select(
        "*",
        F.round("discount_pct", 1).alias("discount_pct_rounded"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

cleaned = (
    valid
    .filter(F.col("ends_on") > F.to_date(F.lit("2023-06-29")))
    .withColumn("ends_on_dayofweek", F.dayofweek("ends_on"))
    .withColumn("adjusted_discount_pct", F.round((F.col("discount_pct") * 0.85), 2))
)

enriched = (
    cleaned
    .withColumn("discount_pct_rounded_avg_6", F.avg("discount_pct_rounded").over(w))
    .withColumn("cumulative_adjusted_discount_pct", F.sum("adjusted_discount_pct").over(w2))
    .withColumn("channel_avg_discount_pct", F.avg("discount_pct").over(w_grp))
    .withColumn("adjusted_discount_pct_percent_rank", F.percent_rank().over(w4))
)

base = (
    enriched
    .filter(F.col("adjusted_discount_pct_percent_rank").between(0.37, 0.55))
    .select(
        "*",
        F.col("ends_on_dayofweek").cast("string").alias("ends_on_dayofweek_num"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_rounded",
        "channel_filled",
        "ends_on_dayofweek",
        "adjusted_discount_pct",
    )
    .orderBy(F.col("discount_pct_rounded").desc_nulls_first())
)
