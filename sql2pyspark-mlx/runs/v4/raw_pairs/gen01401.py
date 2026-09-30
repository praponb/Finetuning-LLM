from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w_grp = Window.partitionBy("plan")
w3 = Window.orderBy(F.col("val").desc())
w4 = Window.partitionBy("currency", "plan")
w5 = Window.partitionBy("currency")
w6 = Window.orderBy(F.col("val").desc_nulls_first())
w7 = Window.partitionBy("plan", "currency")
w8 = Window.orderBy(F.col("val").asc_nulls_last())
w9 = Window.orderBy(F.col("val_rank").desc_nulls_first())
w10 = Window.orderBy(F.col("val_percent_rank_2").asc_nulls_last())
w11 = Window.partitionBy("currency").orderBy(F.col("val_rank").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("val"))
)

metrics = (
    lines
    .withColumn("val_percent_rank", F.percent_rank().over(w))
    .withColumn("plan_count_val", F.count("val").over(w_grp))
    .withColumn("plan_count_val_2", F.count("val").over(w_grp))
    .withColumn("val_rank", F.rank().over(w3))
)

enriched = (
    metrics
    .select(
        "*",
        F.sum("val").over(w4).alias("currency_sum_val"),
        F.stddev_samp("plan_count_val_2").over(w5).alias("currency_stddev_samp_plan_count_val_2"),
    )
)

metrics_price_tiers = (
    enriched
    .withColumn("val_percent_rank_2", F.percent_rank().over(w6))
    .withColumn("plan_count_currency_sum_val", F.count("currency_sum_val").over(w7))
    .withColumn("val_rank_share", F.round((F.col("val_rank") / F.sum("val_rank").over(w5)), 4))
    .withColumn("val_cume_dist", F.cume_dist().over(w8))
    .withColumn("plan_count_val_share", F.round((F.col("plan_count_val") / F.sum("plan_count_val").over(w_grp)), 4))
)

scored = (
    metrics_price_tiers
    .select(
        "*",
        F.rank().over(w9).alias("val_rank_rank"),
        F.percent_rank().over(w10).alias("val_percent_rank_2_percent_rank"),
        F.round((F.col("plan_count_val_2") / F.sum("plan_count_val_2").over(w_grp)), 4).alias("plan_count_val_2_share"),
        F.rank().over(w11).alias("val_rank_rank_2"),
    )
)

result = (
    scored
    .groupBy(
        "currency",
        F.when(F.col("plan_count_val_share") >= 0.85, "LARGE").otherwise("SMALL").alias("plan_count_val_share_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.count("*").alias("event_count"),
        F.sum("val_percent_rank").alias("total_val_percent_rank"),
    )
    .orderBy(F.col("event_count").desc_nulls_first())
)
