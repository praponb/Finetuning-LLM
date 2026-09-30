from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag")
w2 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_recent = Window.partitionBy("tag", "promo").orderBy(F.col("product_id").asc_nulls_first())
w4 = Window.partitionBy("tag", "promo").orderBy(F.col("product_id_rank").desc_nulls_last())

exploded = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

flattened = (
    exploded
    .select(
        "id",
        "created_at",
        "product_id",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

with_windows = (
    flattened
    .filter(F.col("promo").isNotNull())
    .select(
        "created_at",
        "product_id",
        "price",
        "promo",
        "tag",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.min("price").over(w).alias("tag_min_price"),
        F.max("product_id").over(w2).alias("product_id_max_15m"),
        F.rank().over(w_recent).alias("product_id_rank"),
    )
)

with_windows_carts = (
    with_windows
    .select(
        "*",
        F.rank().over(w4).alias("product_id_rank_rank"),
        F.sum("product_id_rank").over(w2).alias("product_id_rank_sum_15m"),
    )
)

result = (
    with_windows_carts
    .groupBy(
        F.when(F.col("tag_min_price") >= 241, "LARGE").otherwise("SMALL").alias("tag_min_price_tier"),
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        "tag",
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("tag") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
    )
    .filter(F.col("welcome5_rows") > 1)
    .orderBy(F.col("tag").desc())
)
