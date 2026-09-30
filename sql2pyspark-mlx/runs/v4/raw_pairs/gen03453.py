from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("currency", "plan")
w4 = Window.partitionBy("plan").orderBy(F.col("currency_count_val").asc_nulls_last())
w5 = Window.orderBy(F.col("val_dense_rank").desc_nulls_last())
w6 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("val"))
)

enriched = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w).alias("val_dense_rank"),
        F.stddev_samp("val").over(w2).alias("plan_stddev_samp_val"),
        F.count("val").over(w3).alias("currency_count_val"),
    )
)

matched_keys = (
    enriched
    .select("id", "currency")
    .intersect(
        df_price_tiers
        .filter(F.col("currency").isin(["USD", "THB", "EUR"]))
        .select("id", "currency")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

enriched_price_tiers = (
    reconciled
    .withColumn("currency_count_val_percent_rank", F.percent_rank().over(w4))
    .withColumn("val_dense_rank_percent_rank", F.percent_rank().over(w5))
    .withColumn("currency_stddev_samp_plan_stddev_samp_val", F.stddev_samp("plan_stddev_samp_val").over(w6))
)

metrics = (
    enriched_price_tiers
    .select(
        "*",
        F.max("currency_count_val_percent_rank").over(w6).alias("currency_max_currency_count_val_percent_rank"),
        F.round((F.col("currency_count_val") / F.sum("currency_count_val").over(w2)), 4).alias("currency_count_val_share"),
        F.avg("val").over(w2).alias("plan_avg_val"),
    )
)

result = (
    metrics
    .select(
        "id",
        "plan",
        "currency",
        "val",
        "val_dense_rank",
        "plan_stddev_samp_val",
        "currency_count_val",
        "currency_count_val_percent_rank",
        "currency_stddev_samp_plan_stddev_samp_val",
        F.when(F.col("currency").rlike("^(EUR|THB)$"), "CORE").otherwise("EXTENDED").alias("currency_zone"),
    )
    .orderBy(F.col("currency_count_val").desc())
)
