from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("val")
w2 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w_grp = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_first())
w4 = Window.partitionBy("currency").orderBy(F.col("val").desc())
w5 = Window.partitionBy(F.month("effective_from"), "currency").orderBy(F.col("val_cume_dist").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_prices").alias("val"))
)

enriched = (
    flattened
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_percent_rank", F.percent_rank().over(w2))
    .withColumn("val_percent_rank_2", F.percent_rank().over(w_grp))
    .withColumn("val_cume_dist", F.cume_dist().over(w_grp))
    .withColumn("val_percent_rank_3", F.percent_rank().over(w4))
)

scored = (
    enriched
    .filter(F.col("val_percent_rank_3") <= 0.37)
    .select(
        "id",
        "effective_from",
        "val_rank",
        "val_percent_rank_2",
        "val_cume_dist",
        F.dense_rank().over(w5).alias("val_cume_dist_dense_rank"),
    )
)

cleaned = (
    scored
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-03-18"))) & (F.col("val_percent_rank_2") > 0.77))
    .select(
        "id",
        "effective_from",
        "val_rank",
        "val_percent_rank_2",
        "val_cume_dist",
        F.date_format("effective_from", "yyyy-MM-dd").alias("effective_from_label"),
        (F.col("val_cume_dist_dense_rank") * F.col("val_cume_dist")).alias("val_cume_dist_dense_rank_times_val_cume_dist"),
    )
)

result = (
    cleaned
    .withColumn("val_cume_dist_tier", F.when(F.col("val_cume_dist") >= 0.69, "LARGE").otherwise("SMALL"))
    .withColumn("val_percent_rank_2_tier", F.when(F.col("val_percent_rank_2") >= 0.74, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["val_cume_dist_tier", "val_percent_rank_2_tier"], ["val_cume_dist_tier"], []],
        "val_cume_dist_tier",
        "val_percent_rank_2_tier",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("val_cume_dist_dense_rank_times_val_cume_dist"), 2).alias("avg_val_cume_dist_dense_rank_times_val_cume_dist"),
        F.percentile_approx("val_cume_dist", [0.25, 0.5, 0.75]).alias("val_cume_dist_quartiles"),
        F.grouping("val_cume_dist_tier").alias("grouping_level"),
    )
    .orderBy(F.col("val_cume_dist_tier").desc())
)
