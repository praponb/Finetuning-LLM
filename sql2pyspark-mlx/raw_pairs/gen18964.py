from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("val")
w2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_first())
w_ordered = Window.partitionBy("currency")

flattened = df_price_tiers.select("id", "currency", F.explode_outer("tier_prices").alias("val"))

with_windows = (
    flattened
    .select(
        "id",
        "currency",
        F.cume_dist().over(w).alias("val_cume_dist"),
        F.cume_dist().over(w2).alias("val_cume_dist_2"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.count("val_cume_dist_2").over(w_ordered).alias("currency_count_val_cume_dist_2"),
        F.max("val_cume_dist_2").over(w_ordered).alias("currency_max_val_cume_dist_2"),
        F.avg("val_cume_dist").over(w_ordered).alias("currency_avg_val_cume_dist"),
    )
)

enriched_price_tiers = (
    enriched
    .withColumn("currency_count_val_cume_dist_2_share", F.round((F.col("currency_count_val_cume_dist_2") / F.sum("currency_count_val_cume_dist_2").over(w_ordered)), 4))
)

valid = (
    enriched_price_tiers
    .filter(F.col("currency_count_val_cume_dist_2_share") >= 0.42)
    .select(
        "id",
        "currency",
        "currency_count_val_cume_dist_2",
        "currency_max_val_cume_dist_2",
        "currency_avg_val_cume_dist",
        (F.col("currency_count_val_cume_dist_2_share") / F.nullif("val_cume_dist", F.lit(0))).alias("currency_count_val_cume_dist_2_share_per_val_cume_dist"),
    )
)

result = (
    valid
    .groupBy("currency")
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.round(F.avg("currency_max_val_cume_dist_2"), 2).alias("avg_currency_max_val_cume_dist_2"),
        F.size(F.collect_set("currency")).alias("num_currencies_2"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
    )
)
