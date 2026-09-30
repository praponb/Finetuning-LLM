from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("channel")
w3 = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_promotions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
        F.sum("discount_pct").over(w3).alias("cumulative_discount_pct"),
    )
)

kept_keys = (
    metrics
    .select("promo_code", "channel")
    .subtract(
        df_promotions
        .filter(F.col("starts_on") >= "2024-05-01")
        .select("promo_code", "channel")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("discount_pct_share") >= 0.66, "PREMIUM").otherwise("STANDARD").alias("discount_pct_share_tier"),
    )
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.max(F.when(F.col("channel") == "partner", F.col("discount_pct_share"))).alias("partner_max_discount_pct_share"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
