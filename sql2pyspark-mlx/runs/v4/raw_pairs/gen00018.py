from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("discount_pct_tier").orderBy(F.col("store_events").desc())

result = (
    df_promotions
    .groupBy(
        F.when(F.col("discount_pct") >= 0.36, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
        F.date_trunc("WEEK", "starts_on").alias("week_start"),
    )
    .agg(
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.avg(F.when(F.col("promo_code") == "SPRING10", F.col("discount_pct"))).alias("spring10_avg_discount_pct"),
    )
    .withColumn("store_events_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("week_start").asc_nulls_first())
)
