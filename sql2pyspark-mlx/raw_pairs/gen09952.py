from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_recent = Window.partitionBy(F.to_date("created_at"), "promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy(F.month("created_at")).orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("promo").orderBy(F.col("qty").desc_nulls_first())
w6 = Window.partitionBy(F.to_date("created_at"), "promo")
w_part = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w8 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_part_2 = Window.partitionBy(F.to_date("created_at"), "promo").orderBy(F.col("qty_rank").desc_nulls_first())
w10 = Window.partitionBy(F.year("created_at"), "promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w11 = Window.partitionBy("promo").orderBy(F.col("promo_min_promo_avg_qty").desc_nulls_last())

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.promo"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.avg("qty").over(w_ordered).alias("promo_avg_qty"),
        F.round((F.col("qty") / F.sum("qty").over(w_ordered)), 4).alias("qty_share"),
        F.max("product_id").over(w2).alias("product_id_max_30m"),
    )
)

with_windows_carts = (
    with_windows
    .withColumn("qty_avg_15m", F.avg("qty").over(w_recent))
    .withColumn("qty_max_1h", F.max("qty").over(w4))
    .withColumn("qty_rank", F.rank().over(w5))
    .withColumn("promo_avg_qty_share", F.avg("qty_share").over(w_ordered))
)

valid = (
    with_windows_carts
    .filter(F.col("created_at") >= F.to_date(F.lit("2025-03-01")))
    .withColumn("least_promo_avg_qty_share_qty_share", F.least("promo_avg_qty_share", "qty_share"))
    .withColumn("product_id_max_30m_level", F.when(F.col("product_id_max_30m") >= 9, "High").when(F.col("product_id_max_30m") >= 3, "Medium").otherwise("Low"))
)

with_windows_carts_2 = (
    valid
    .filter(F.col("product_id").between(5, 9))
    .select(
        "*",
        F.max("product_id_max_30m").over(w6).alias("created_at_to_date_max_product_id_max_30m"),
        F.avg("promo_avg_qty_share").over(w_part).alias("promo_avg_qty_share_avg_15m"),
        F.max("promo_avg_qty_share").over(w8).alias("promo_avg_qty_share_max_2h"),
        F.min("promo_avg_qty").over(w_ordered).alias("promo_min_promo_avg_qty"),
        F.percent_rank().over(w_part_2).alias("qty_rank_percent_rank"),
    )
)

metrics = (
    with_windows_carts_2
    .select(
        "*",
        F.sum("product_id_max_30m").over(w10).alias("product_id_max_30m_sum_15m"),
        F.rank().over(w11).alias("promo_min_promo_avg_qty_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        F.when(F.col("product_id_max_30m") >= 10, "HIGH").otherwise("LOW").alias("product_id_max_30m_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("promo") == "VIP", F.col("qty_share")).otherwise(0)).alias("vip_qty_share"),
        F.countDistinct("promo").alias("distinct_promos"),
    )
    .filter(F.col("distinct_promos") >= 1)
    .orderBy(F.col("product_id_max_30m_tier").desc())
)
