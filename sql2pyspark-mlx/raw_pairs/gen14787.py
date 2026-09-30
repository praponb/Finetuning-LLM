from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("quantity", "id")
w4 = Window.orderBy(F.col("unit_price").desc(), "id")
w_grp = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-5, Window.currentRow)
w_part = Window.partitionBy("product_id", "order_id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([3, 23, 32, 39]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.floor("quantity").alias("quantity_whole"),
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.avg("quantity_whole").over(w).alias("order_id_avg_quantity_whole"),
        F.first("quantity", ignorenulls=True).over(w2).alias("first_known_quantity"),
    )
)

metrics_order_items = (
    metrics
    .select(
        "*",
        F.ntile(4).over(w3).alias("quantity_bucket"),
        F.ntile(10).over(w4).alias("unit_price_bucket"),
        F.avg("unit_price").over(w_grp).alias("unit_price_avg_5"),
        F.stddev_samp("first_known_quantity").over(w_part).alias("product_id_stddev_samp_first_known_quantity"),
    )
)

with_dims = (
    metrics_order_items
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.product_id",
        "s.quantity",
        "s.unit_price",
        "s.unit_price_filled",
        "s.unit_price_bucket",
        "s.unit_price_avg_5",
        "s.product_id_stddev_samp_first_known_quantity",
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("p.brand"),
    )
)

base = (
    with_dims
    .filter(F.col("unit_price_avg_5") >= 190)
    .select(
        "*",
        F.ceil("product_id_stddev_samp_first_known_quantity").alias("product_id_stddev_samp_first_known_quantity_whole"),
        (F.col("stock_qty") + F.col("unit_price_avg_5")).alias("stock_qty_plus_unit_price_avg_5"),
    )
)

result = (
    base
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .cube(
        F.col("p.brand").alias("product_brand"),
        F.col("s.brand").alias("order_item_brand"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.product_id_stddev_samp_first_known_quantity")).alias("min_product_id_stddev_samp_first_known_quantity"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("min_product_id_stddev_samp_first_known_quantity").desc_nulls_last())
)
