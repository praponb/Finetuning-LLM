from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_first())
w2 = Window.orderBy(F.col("val").desc())
w3 = Window.partitionBy("plan")
w_recent = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_last())
w_grp = Window.partitionBy("plan").orderBy(F.col("val_rank").desc())
w6 = Window.partitionBy("currency").orderBy(F.col("val_rank_dense_rank").asc_nulls_first())
w7 = Window.partitionBy("plan", "currency")
w8 = Window.orderBy(F.col("val").desc_nulls_first())
w9 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("val"))
)

enriched = (
    exploded
    .select(
        "*",
        F.rank().over(w).alias("val_rank"),
        F.rank().over(w2).alias("val_rank_2"),
    )
)

scored = (
    enriched
    .filter(F.col("currency").isin(["EUR", "THB", "USD"]))
    .withColumn("plan_sum_val", F.sum("val").over(w3))
    .withColumn("val_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("val_rank_dense_rank", F.dense_rank().over(w_grp))
)

scored_price_tiers = (
    scored
    .withColumn("val_rank_dense_rank_cume_dist", F.cume_dist().over(w6))
    .withColumn("plan_min_val_rank_2", F.min("val_rank_2").over(w7))
    .withColumn("val_percent_rank", F.percent_rank().over(w8))
    .withColumn("currency_avg_val", F.avg("val").over(w9))
)

result = (
    scored_price_tiers
    .groupBy(
        F.when(F.col("val_rank_2") >= 8, "LARGE").otherwise("SMALL").alias("val_rank_2_tier"),
        F.when(F.col("plan_min_val_rank_2") >= 9, "LARGE").otherwise("SMALL").alias("plan_min_val_rank_2_tier"),
    )
    .agg(
        F.sum("val_rank_2").alias("total_val_rank_2"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_rows"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
    )
)
