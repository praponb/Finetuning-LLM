from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on").desc(), "id")

latest = (
    df_promotions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy("promo_code", F.date_trunc("MONTH", "starts_on").alias("activity_month"))
    .agg(
        F.percentile_approx("discount_pct", 0.5).alias("discount_pct_p50"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.max("discount_pct").alias("max_discount_pct"),
    )
    .filter(F.col("discount_pct_p50") > 0)
    .orderBy(F.col("activity_month").desc_nulls_last())
)
