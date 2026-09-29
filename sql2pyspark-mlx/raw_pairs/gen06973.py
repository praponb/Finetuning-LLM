from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())

latest = (
    df_promotions
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .groupBy(F.date_trunc("MONTH", "ends_on").alias("activity_month"))
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("discount_pct_per_welcome5"),
        F.max(F.when(F.col("channel") == "mobile", F.col("discount_pct"))).alias("mobile_max_discount_pct"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.avg(F.when(F.col("channel") == "web", F.col("discount_pct"))).alias("web_avg_discount_pct"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev_2"),
    )
)
