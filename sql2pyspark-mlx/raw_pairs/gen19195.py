from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("prev_discount_pct_rounded").asc_nulls_first())
w3 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct_rounded").desc(), "id")

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & F.col("channel").isin(["mobile", "store", "web"]))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.round("discount_pct", 2).alias("discount_pct_rounded"),
        F.round((F.col("discount_pct") * 0.9), 2).alias("adjusted_discount_pct"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

enriched = (
    filtered
    .filter(F.col("ends_on").between("2023-03-07", "2023-10-07"))
    .select(
        "*",
        F.lead("adjusted_discount_pct").over(w).alias("next_adjusted_discount_pct"),
        F.lead("discount_pct_rounded").over(w).alias("next_discount_pct_rounded"),
        F.lag("discount_pct_rounded").over(w).alias("prev_discount_pct_rounded"),
        F.row_number().over(w).alias("row_seq"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

latest_promotions = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest_promotions
    .groupBy("promo_code")
    .pivot("channel", ["mobile", "partner", "web"])
    .agg(F.avg("next_discount_pct_rounded").alias("value_sum"))
)
