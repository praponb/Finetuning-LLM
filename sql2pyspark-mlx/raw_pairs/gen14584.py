from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").asc_nulls_last())
w_recent = Window.partitionBy(F.to_date("created_at"), "promo")
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.orderBy(F.col("created_at_to_date_max_product_id").desc_nulls_first())
w6 = Window.orderBy(F.col("qty_rank").desc())
w7 = Window.orderBy("created_at_to_date_avg_qty")
w8 = Window.orderBy(F.col("created_at_to_date_max_product_id_avg_1h").desc_nulls_first())

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "created_at",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

scored = (
    exploded
    .select(
        "id",
        "created_at",
        F.rank().over(w).alias("qty_rank"),
        F.max("product_id").over(w_recent).alias("created_at_to_date_max_product_id"),
        F.avg("qty").over(w_recent).alias("created_at_to_date_avg_qty"),
    )
)

metrics = (
    scored
    .select(
        "qty_rank",
        "created_at_to_date_max_product_id",
        "created_at_to_date_avg_qty",
        F.sum("qty_rank").over(w3).alias("qty_rank_sum_15m"),
        F.avg("created_at_to_date_max_product_id").over(w4).alias("created_at_to_date_max_product_id_avg_1h"),
        F.percent_rank().over(w5).alias("created_at_to_date_max_product_id_percent_rank"),
        F.rank().over(w6).alias("qty_rank_rank"),
    )
)

scored_carts = (
    metrics
    .withColumn("created_at_to_date_avg_qty_rank", F.rank().over(w7))
    .withColumn("created_at_to_date_max_product_id_avg_1h_percent_rank", F.percent_rank().over(w8))
)

valid = (
    scored_carts
    .filter(F.col("created_at_to_date_avg_qty_rank") > 8)
    .select(
        "created_at_to_date_avg_qty",
        "qty_rank_sum_15m",
        "created_at_to_date_max_product_id_avg_1h",
        "qty_rank_rank",
        "created_at_to_date_avg_qty_rank",
        "created_at_to_date_max_product_id_avg_1h_percent_rank",
        (F.col("created_at_to_date_avg_qty_rank") * F.col("created_at_to_date_max_product_id")).alias("created_at_to_date_avg_qty_rank_times_created_at_to_date_max_product_id"),
        F.round((F.col("qty_rank_sum_15m") / F.col("created_at_to_date_max_product_id")), 2).alias("qty_rank_sum_15m_per_created_at_to_date_max_product_id"),
    )
)

result = (
    valid
    .select(
        "created_at_to_date_avg_qty",
        "qty_rank_sum_15m",
        "created_at_to_date_max_product_id_avg_1h",
        "created_at_to_date_avg_qty_rank",
        "qty_rank_sum_15m_per_created_at_to_date_max_product_id",
    )
)
