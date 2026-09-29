from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("currency_avg_val").desc())
w_ordered = Window.partitionBy("currency").orderBy(F.col("currency_avg_val").desc_nulls_first())
w_grp = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w5 = Window.partitionBy("currency").orderBy("val")
w6 = Window.orderBy(F.col("currency_avg_val_rank").desc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

scored = (
    flattened
    .filter(F.col("currency").isin(["EUR", "THB"]))
    .withColumn("currency_max_val", F.max("val").over(w_recent))
    .withColumn("currency_avg_val", F.avg("val").over(w_recent))
    .withColumn("currency_avg_val_2", F.avg("val").over(w_recent))
)

metrics = (
    scored
    .withColumn("currency_avg_val_rank", F.rank().over(w2))
    .withColumn("currency_avg_val_percent_rank", F.percent_rank().over(w_ordered))
    .withColumn("val_rank", F.rank().over(w_grp))
    .withColumn("currency_min_currency_avg_val_2", F.min("currency_avg_val_2").over(w_recent))
    .withColumn("val_cume_dist", F.cume_dist().over(w5))
)

metrics_price_tiers = (
    metrics
    .select(
        "effective_from",
        "currency",
        "val",
        "currency_avg_val_2",
        "currency_avg_val_rank",
        "currency_avg_val_percent_rank",
        "val_cume_dist",
        F.avg("currency_max_val").over(w_recent).alias("currency_avg_currency_max_val"),
        F.avg("currency_avg_val").over(w_recent).alias("currency_avg_currency_avg_val"),
        F.round((F.col("currency_avg_val_rank") / F.sum("currency_avg_val_rank").over(w_recent)), 4).alias("currency_avg_val_rank_share"),
        F.dense_rank().over(w6).alias("currency_avg_val_rank_dense_rank"),
    )
)

result = (
    metrics_price_tiers
    .select(
        "effective_from",
        "currency",
        "currency_avg_val_2",
        "currency_avg_val_percent_rank",
        "val_cume_dist",
        "currency_avg_currency_max_val",
        "currency_avg_currency_avg_val",
        "currency_avg_val_rank_share",
        "currency_avg_val_rank_dense_rank",
        F.when(F.col("currency").rlike("^(USD|EUR)$"), "GROUP_B").when(F.col("currency").rlike("^(THB)$"), "EXTENDED").otherwise("PRIORITY").alias("currency_zone"),
    )
)
