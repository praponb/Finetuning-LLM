from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("qty").desc_nulls_first())
w2 = Window.partitionBy("promo").orderBy("qty_rank")
w3 = Window.partitionBy("promo").orderBy(F.col("qty_per_price").asc_nulls_first())
w4 = Window.orderBy(F.col("qty_per_price").desc_nulls_last())
w5 = Window.orderBy(F.col("qty_per_price").asc_nulls_first())
w_hist = Window.orderBy(F.col("qty_per_price_cume_dist").desc_nulls_first())
w_hist_2 = Window.orderBy(F.col("qty_rank_dense_rank").desc())
w_grp = Window.orderBy(F.col("qty_per_price_dense_rank").asc_nulls_last())

flattened = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "created_at",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

valid = (
    flattened
    .filter((~F.col("promo").eqNullSafe(F.lit("WELCOME5"))) & (F.col("created_at") >= F.lit("2025-03-01").cast("date")))
    .select(
        "qty",
        "promo",
        F.round((F.col("qty") / F.col("price")), 2).alias("qty_per_price"),
        F.coalesce("promo", F.lit("DIRECT")).alias("resolved_promo"),
    )
)

scored = valid.select("*", F.rank().over(w).alias("qty_rank"))

with_windows = (
    scored
    .filter(F.col("qty_rank").between(5, 6))
    .select(
        "qty",
        "qty_rank",
        F.dense_rank().over(w2).alias("qty_rank_dense_rank"),
        F.cume_dist().over(w3).alias("qty_per_price_cume_dist"),
        F.percent_rank().over(w4).alias("qty_per_price_percent_rank"),
        F.dense_rank().over(w5).alias("qty_per_price_dense_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "qty",
        "qty_rank_dense_rank",
        "qty_per_price_percent_rank",
        "qty_per_price_dense_rank",
        F.cume_dist().over(w_hist).alias("qty_per_price_cume_dist_cume_dist"),
        F.rank().over(w_hist_2).alias("qty_rank_dense_rank_rank"),
        F.dense_rank().over(w_grp).alias("qty_per_price_dense_rank_dense_rank"),
        F.rank().over(w_hist).alias("qty_per_price_cume_dist_rank"),
    )
)

valid_carts = (
    enriched
    .filter(F.col("qty_per_price_cume_dist_rank") == 5)
    .withColumn("qty_rank_dense_rank_rank_plus_qty_per_price_percent_rank", (F.col("qty_rank_dense_rank_rank") + F.col("qty_per_price_percent_rank")))
)

result = (
    valid_carts
    .select(
        "qty_rank_dense_rank",
        "qty_per_price_percent_rank",
        "qty_per_price_dense_rank",
        "qty_per_price_cume_dist_cume_dist",
        "qty_per_price_dense_rank_dense_rank",
        "qty_per_price_cume_dist_rank",
    )
)
