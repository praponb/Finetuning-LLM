from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").asc_nulls_last())
w2 = Window.partitionBy("promo")
w3 = Window.orderBy(F.col("product_id").asc_nulls_last())
w4 = Window.orderBy("qty_cume_dist")
w5 = Window.orderBy(F.col("qty_cume_dist_rank").desc_nulls_first())
w6 = Window.orderBy(F.col("qty_share_2").desc())
w7 = Window.orderBy(F.col("product_id").desc_nulls_last())

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select("id", F.col("item.product_id"), F.col("item.qty"), F.col("item.promo"))
)

cleaned = (
    exploded
    .filter(F.col("promo") == "BUNDLE")
    .withColumn("promo_group", F.when(F.col("promo").isin(["VIP", "WELCOME5"]), "Group A").otherwise("Other"))
    .withColumn("promo_filled", F.coalesce("promo", F.lit("N/A")))
)

with_windows = (
    cleaned
    .select(
        "id",
        "product_id",
        "qty",
        F.cume_dist().over(w).alias("qty_cume_dist"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share"),
        F.percent_rank().over(w3).alias("product_id_percent_rank"),
        F.count("product_id").over(w2).alias("promo_count_product_id"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share_2"),
    )
)

with_windows_carts = with_windows.withColumn("qty_cume_dist_rank", F.rank().over(w4))

with_windows_carts_2 = (
    with_windows_carts
    .withColumn("qty_cume_dist_rank_rank", F.rank().over(w5))
    .withColumn("qty_share_2_rank", F.rank().over(w6))
    .withColumn("product_id_cume_dist", F.cume_dist().over(w7))
)

cleaned_carts = (
    with_windows_carts_2
    .filter(F.col("qty_cume_dist_rank") > 3)
    .select(
        "product_id",
        "qty",
        "qty_cume_dist",
        "qty_share",
        "product_id_percent_rank",
        "qty_share_2",
        "qty_cume_dist_rank",
        "qty_cume_dist_rank_rank",
        "qty_share_2_rank",
        F.col("qty_share_2_rank").cast("string").alias("qty_share_2_rank_num"),
        (F.col("qty_share_2_rank") - F.col("product_id_cume_dist")).alias("qty_share_2_rank_minus_product_id_cume_dist"),
    )
)

valid = (
    cleaned_carts
    .filter(F.col("qty_share_2_rank_minus_product_id_cume_dist").between(26, 63))
    .withColumn("qty_share_2_rank_plus_qty_share_2_rank_minus_product_id_cume_dist", (F.col("qty_share_2_rank") + F.col("qty_share_2_rank_minus_product_id_cume_dist")))
    .withColumn("qty_share_filled", F.coalesce("qty_share", F.lit(0)))
)

result = (
    valid
    .select(
        "product_id",
        "qty_cume_dist",
        "qty_share",
        "qty_share_2",
        "qty_cume_dist_rank",
        "qty_share_2_rank_num",
        "qty_share_2_rank_plus_qty_share_2_rank_minus_product_id_cume_dist",
    )
    .orderBy(F.col("qty_share_2").asc_nulls_last())
)
