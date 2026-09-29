from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("running_unit_price").asc_nulls_first())
w4 = Window.partitionBy("order_id")

with_windows = (
    df_order_items
    .select(
        "id",
        "order_id",
        F.stddev_samp("quantity").over(w_hist).alias("product_id_stddev_samp_quantity"),
        F.sum("unit_price").over(w2).alias("running_unit_price"),
    )
)

remaining_keys = (
    with_windows
    .select("order_id", "id")
    .exceptAll(
        df_order_items
        .filter(F.col("unit_price") < 170)
        .select("order_id", "id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows_order_items = (
    kept_rows
    .filter(F.col("product_id_stddev_samp_quantity").between(6, 8))
    .select(
        "*",
        F.dense_rank().over(w3).alias("running_unit_price_dense_rank"),
        F.stddev_samp("running_unit_price").over(w4).alias("order_id_stddev_samp_running_unit_price"),
        F.round((F.col("running_unit_price") / F.sum("running_unit_price").over(w4)), 4).alias("running_unit_price_share"),
    )
)

valid = (
    with_windows_order_items
    .filter(F.col("product_id_stddev_samp_quantity") != 3)
    .select(
        "*",
        (F.col("running_unit_price_share") - F.col("running_unit_price_dense_rank")).alias("running_unit_price_share_minus_running_unit_price_dense_rank"),
        (F.col("running_unit_price") - F.col("product_id_stddev_samp_quantity")).alias("running_unit_price_minus_product_id_stddev_samp_quantity"),
    )
)

matched_keys = (
    valid
    .select("order_id")
    .exceptAll(
        df_order_items
        .filter(F.col("quantity") == 6)
        .select("order_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("running_unit_price_share_minus_running_unit_price_dense_rank") >= 79, "PREMIUM").otherwise("STANDARD").alias("running_unit_price_share_minus_running_unit_price_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("running_unit_price_dense_rank", [0.25, 0.5, 0.75]).alias("running_unit_price_dense_rank_quartiles"),
        F.percentile_approx("product_id_stddev_samp_quantity", 0.95).alias("product_id_stddev_samp_quantity_p95"),
        F.percentile_approx("product_id_stddev_samp_quantity", 0.95).alias("product_id_stddev_samp_quantity_p95_2"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("product_id_stddev_samp_quantity", 0.5).alias("product_id_stddev_samp_quantity_p50"),
        F.countDistinct("order_id").alias("distinct_order_ids_2"),
    )
    .orderBy(F.col("product_id_stddev_samp_quantity_p50").desc_nulls_last())
)
