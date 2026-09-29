from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy(F.date_trunc("MONTH", "created_at"), "promo")
w3 = Window.partitionBy(F.to_date("created_at"), "promo")
w4 = Window.partitionBy("promo")
w_running = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_recent = Window.orderBy(F.col("created_at_period_max_product_id").desc_nulls_first())
w_running_2 = Window.orderBy(F.col("qty_share").desc_nulls_first())
w8 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_part = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_grp = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w11 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w12 = Window.orderBy(F.col("created_at_to_date_avg_product_id_share_max_15m").asc_nulls_first())

lines = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

scored = (
    lines
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "qty",
        "price",
        "promo",
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
        F.max("product_id").over(w2).alias("created_at_period_max_product_id"),
        F.avg("product_id").over(w3).alias("created_at_to_date_avg_product_id"),
    )
)

scored_carts = (
    scored
    .select(
        "id",
        "created_at",
        "price",
        "promo",
        "qty_share",
        "created_at_period_max_product_id",
        "created_at_to_date_avg_product_id",
        F.round((F.col("created_at_to_date_avg_product_id") / F.sum("created_at_to_date_avg_product_id").over(w4)), 4).alias("created_at_to_date_avg_product_id_share"),
        F.max("created_at_to_date_avg_product_id").over(w_running).alias("created_at_to_date_avg_product_id_max_30m"),
        F.dense_rank().over(w_recent).alias("created_at_period_max_product_id_dense_rank"),
        F.cume_dist().over(w_running_2).alias("qty_share_cume_dist"),
    )
)

scored_carts_2 = (
    scored_carts
    .filter(F.col("created_at_period_max_product_id_dense_rank").isNotNull())
    .select(
        "created_at",
        "qty_share",
        "created_at_to_date_avg_product_id_max_30m",
        "created_at_period_max_product_id_dense_rank",
        "qty_share_cume_dist",
        F.max("created_at_to_date_avg_product_id_share").over(w8).alias("created_at_to_date_avg_product_id_share_max_15m"),
        F.count("qty_share").over(w_part).alias("qty_share_count_30m"),
        F.max("created_at_period_max_product_id_dense_rank").over(w_grp).alias("created_at_period_max_product_id_dense_rank_max_1h"),
    )
)

enriched = (
    scored_carts_2
    .select(
        "*",
        F.count("qty_share_count_30m").over(w11).alias("qty_share_count_30m_count_2h"),
        F.dense_rank().over(w12).alias("created_at_to_date_avg_product_id_share_max_15m_dense_rank"),
        F.count("qty_share_count_30m").over(w_grp).alias("qty_share_count_30m_count_1h"),
    )
)

result = (
    enriched
    .select(
        "created_at",
        "qty_share",
        "created_at_period_max_product_id_dense_rank",
        "qty_share_cume_dist",
        "created_at_to_date_avg_product_id_share_max_15m",
        "qty_share_count_30m",
        "created_at_period_max_product_id_dense_rank_max_1h",
        "qty_share_count_30m_count_2h",
        "created_at_to_date_avg_product_id_share_max_15m_dense_rank",
    )
    .orderBy(F.col("created_at_period_max_product_id_dense_rank_max_1h").desc())
)
