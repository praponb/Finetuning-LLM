from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy(F.col("qty").desc())
w3 = Window.orderBy(F.col("qty").desc_nulls_first())
w4 = Window.orderBy(F.col("promo_count_qty").asc_nulls_first())
w5 = Window.orderBy("qty_cume_dist")
w6 = Window.partitionBy("promo").orderBy(F.col("promo_count_qty").desc())
w7 = Window.orderBy(F.col("promo_count_qty_cume_dist").desc_nulls_first())

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select("id", "coupon_codes", F.col("item.qty"), F.col("item.promo"))
)

scored = (
    exploded
    .select(
        "id",
        "promo",
        F.count("qty").over(w).alias("promo_count_qty"),
        F.percent_rank().over(w2).alias("qty_percent_rank"),
        F.cume_dist().over(w3).alias("qty_cume_dist"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.count("promo_count_qty").over(w).alias("promo_count_promo_count_qty"),
        F.cume_dist().over(w4).alias("promo_count_qty_cume_dist"),
        F.sum("qty_cume_dist").over(w).alias("promo_sum_qty_cume_dist"),
        F.dense_rank().over(w5).alias("qty_cume_dist_dense_rank"),
    )
)

base = (
    enriched
    .filter(F.col("promo").isNotNull() & (F.col("qty_cume_dist_dense_rank") != 7))
    .select("*", F.coalesce("promo", F.lit("N/A")).alias("promo_filled"))
)

valid = (
    base
    .filter(F.col("promo").isNotNull())
    .select(
        "id",
        "promo",
        "promo_count_qty",
        "qty_percent_rank",
        "qty_cume_dist",
        "promo_count_promo_count_qty",
        "promo_count_qty_cume_dist",
        "promo_sum_qty_cume_dist",
        "qty_cume_dist_dense_rank",
        (F.col("qty_percent_rank") - F.col("promo_count_qty")).alias("qty_percent_rank_minus_promo_count_qty"),
        F.col("qty_cume_dist_dense_rank").cast("string").alias("qty_cume_dist_dense_rank_str"),
    )
)

result = (
    valid
    .select(
        "promo",
        "qty_percent_rank",
        "qty_cume_dist",
        "promo_count_qty_cume_dist",
        "promo_sum_qty_cume_dist",
        "qty_cume_dist_dense_rank",
        "qty_cume_dist_dense_rank_str",
        F.dense_rank().over(w6).alias("promo_count_qty_dense_rank"),
        F.cume_dist().over(w7).alias("promo_count_qty_cume_dist_cume_dist"),
        F.dense_rank().over(w4).alias("promo_count_qty_dense_rank_2"),
        F.max("promo_count_promo_count_qty").over(w).alias("promo_max_promo_count_promo_count_qty"),
        F.round((F.col("promo_count_qty") / F.sum("promo_count_qty").over(w)), 4).alias("promo_count_qty_share"),
    )
)
