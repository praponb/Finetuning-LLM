from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w_recent = Window.partitionBy("plan")
w4 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())
w5 = Window.partitionBy("currency").orderBy(F.col("plan_stddev_samp_tier_value").desc_nulls_first())
w6 = Window.partitionBy("plan").orderBy("tier_value_percent_rank")
w7 = Window.orderBy(F.col("currency_min_tier_value").asc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("tier_value"))
)

enriched = (
    exploded
    .withColumn("currency_min_tier_value", F.min("tier_value").over(w))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
    .withColumn("plan_stddev_samp_tier_value", F.stddev_samp("tier_value").over(w_recent))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w4))
)

scored = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w5).alias("plan_stddev_samp_tier_value_percent_rank"),
        F.cume_dist().over(w6).alias("tier_value_percent_rank_cume_dist"),
        F.cume_dist().over(w7).alias("currency_min_tier_value_cume_dist"),
    )
)

cleaned = (
    scored
    .filter(F.col("tier_value_dense_rank") == 8)
    .withColumn("plan_stddev_samp_tier_value_band", F.when(F.col("plan_stddev_samp_tier_value") >= 87, "High").when(F.col("plan_stddev_samp_tier_value") >= 21, "Medium").otherwise("Low"))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("tier_value") >= 74, "HIGH").otherwise("LOW").alias("tier_value_tier"),
        F.when(F.col("currency_min_tier_value") >= 63, "LARGE").otherwise("SMALL").alias("currency_min_tier_value_tier"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "pro", F.col("tier_value_percent_rank"))).alias("pro_total_tier_value_percent_rank"),
        F.avg(F.when(F.col("currency") == "THB", F.col("plan_stddev_samp_tier_value"))).alias("thb_avg_plan_stddev_samp_tier_value"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
)
