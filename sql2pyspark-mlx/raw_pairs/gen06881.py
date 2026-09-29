from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("order_id")
w_recent_2 = Window.partitionBy("product_id", "order_id")
w_recent_3 = Window.orderBy(F.col("unit_price").desc_nulls_last())
w4 = Window.partitionBy("product_id", "order_id").orderBy(F.col("discount").asc_nulls_first())
w_hist = Window.partitionBy("product_id").orderBy(F.col("discount").desc_nulls_first())
w6 = Window.partitionBy("product_id", "order_id").orderBy("discount")
w7 = Window.partitionBy("product_id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_num", F.col("quantity").cast("string"))
)

enriched = (
    filtered
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.discount"), F.col("b.order_date"))
)

kept_keys = (
    enriched
    .select("product_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price") > 320)
        .select("product_id", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_order_items = (
    kept_rows
    .select(
        "id",
        "order_id",
        "product_id",
        "discount",
        F.avg("unit_price").over(w_recent).alias("order_id_avg_unit_price"),
        F.corr("quantity", "unit_price").over(w_recent_2).alias("corr_quantity_unit_price"),
        F.dense_rank().over(w_recent_3).alias("unit_price_dense_rank"),
    )
)

with_windows = (
    enriched_order_items
    .select(
        "*",
        F.round((F.col("unit_price_dense_rank") / F.sum("unit_price_dense_rank").over(w_recent)), 4).alias("unit_price_dense_rank_share"),
        F.dense_rank().over(w4).alias("discount_dense_rank"),
    )
)

with_windows_order_items = (
    with_windows
    .withColumn("discount_rank", F.rank().over(w_hist))
    .withColumn("discount_rank_2", F.rank().over(w6))
    .withColumn("product_id_min_corr_quantity_unit_price", F.min("corr_quantity_unit_price").over(w7))
)

with_dims = (
    with_windows_order_items
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.discount").between(0.12, 0.3))
    .select(
        "a.id",
        "a.order_id",
        "a.discount",
        "a.order_id_avg_unit_price",
        "a.corr_quantity_unit_price",
        "a.discount_dense_rank",
        "a.discount_rank",
        "a.product_id_min_corr_quantity_unit_price",
        F.col("d.status"),
        F.col("d.discount").alias("order_discount"),
        F.col("d.amount"),
    )
)

result = (
    with_dims
    .select(
        "order_id",
        "order_id_avg_unit_price",
        "corr_quantity_unit_price",
        "discount_dense_rank",
        "discount_rank",
        "order_discount",
    )
    .orderBy("discount_dense_rank", F.col("order_discount").desc())
)
