from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id")

deduped = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("discount_pct") >= 0.33, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.sum(F.when(F.col("promo_code") == "VIP", F.col("discount_pct"))).alias("vip_total_discount_pct"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("discount_pct_per_store"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.count_if(F.col("promo_code") == "BUNDLE").alias("bundle_events"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
    )
    .orderBy(F.col("bundle_events").desc())
)
