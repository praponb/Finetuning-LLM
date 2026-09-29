from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "starts_on"), "channel").orderBy("starts_on", "id")
w2 = Window.partitionBy(F.date_trunc("month", "ends_on"), "promo_code").orderBy("ends_on", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("promo_code", "channel")

with_windows = (
    df_promotions
    .filter(F.col("ends_on") < "2023-12-01")
    .select(
        "id",
        "discount_pct",
        F.lag("discount_pct", 2, 0).over(w).alias("prev_discount_pct"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_6_prior"),
        F.max("discount_pct").over(w3).alias("promo_code_max_discount_pct"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("prev_discount_pct") >= 0.28, "LARGE").otherwise("SMALL").alias("prev_discount_pct_tier"),
        F.when(F.col("discount_pct") >= 0.34, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
    )
    .agg(
        F.percentile_approx("discount_pct_avg_6_prior", 0.9).alias("discount_pct_avg_6_prior_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("discount_pct_tier").desc())
)
