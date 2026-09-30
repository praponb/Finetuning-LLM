from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "promo")
w2 = Window.partitionBy("promo", "customer_id").orderBy("qty")
w3 = Window.partitionBy("promo")
w_running = Window.partitionBy("promo").orderBy(F.col("qty").asc_nulls_first())

flattened = df_carts.select("id", "customer_id", F.inline("items"))

with_windows = (
    flattened
    .filter(F.col("price").between(210, 240))
    .select(
        "id",
        "qty",
        "promo",
        F.sum("qty").over(w).alias("customer_id_sum_qty"),
        F.rank().over(w2).alias("qty_rank"),
        F.round((F.col("qty") / F.sum("qty").over(w3)), 4).alias("qty_share"),
        F.percent_rank().over(w_running).alias("qty_percent_rank"),
    )
)

base = (
    with_windows
    .filter(F.col("customer_id_sum_qty").isNull())
    .select(
        "*",
        F.col("qty_share").cast("int").alias("qty_share_int"),
        F.coalesce("promo", F.lit("DIRECT")).alias("promo_filled"),
    )
)

valid = (
    base
    .filter(F.col("promo").isNotNull() & (F.col("qty_share_int") <= 62))
    .select(
        "id",
        "qty",
        "promo",
        "customer_id_sum_qty",
        "qty_rank",
        "qty_percent_rank",
        "qty_share_int",
        F.round((F.col("qty_percent_rank") / F.col("qty")), 2).alias("qty_percent_rank_per_qty"),
    )
)

base_carts = (
    valid
    .filter(F.col("promo").isNotNull())
    .select(
        "*",
        (F.col("qty_percent_rank") * F.col("qty_share_int")).alias("qty_percent_rank_times_qty_share_int"),
        F.round((F.col("qty_percent_rank_per_qty") * 1.07), 2).alias("qty_percent_rank_per_qty_adjusted"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("promo_clean"),
    )
)

base_carts_2 = (
    base_carts
    .filter(F.col("promo").isNotNull())
    .select(
        "id",
        "qty",
        "promo",
        "customer_id_sum_qty",
        "qty_rank",
        "qty_percent_rank",
        "qty_share_int",
        "qty_percent_rank_per_qty",
        "qty_percent_rank_times_qty_share_int",
        "qty_percent_rank_per_qty_adjusted",
        "promo_clean",
        F.length("promo").alias("promo_length"),
    )
)

result = (
    base_carts_2
    .select(
        "qty",
        "customer_id_sum_qty",
        "qty_share_int",
        "qty_percent_rank_times_qty_share_int",
        "qty_percent_rank_per_qty_adjusted",
        F.round((F.col("qty_rank") / F.sum("qty_rank").over(w3)), 4).alias("qty_rank_share"),
    )
)
