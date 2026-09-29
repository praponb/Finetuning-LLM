from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id", "product_id")
w3 = Window.orderBy("running_avg_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("running_avg_unit_price").desc())
w5 = Window.partitionBy("product_id").orderBy("running_max_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_band", F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low"))
)

scored = (
    valid
    .filter(F.col("unit_price") > 310)
    .select(
        "*",
        F.max("unit_price").over(w_recent).alias("running_max_unit_price"),
        F.min("unit_price").over(w_recent).alias("running_min_unit_price"),
        F.sum("unit_price").over(w_recent).alias("running_sum_unit_price"),
        F.avg("unit_price").over(w_recent).alias("running_avg_unit_price"),
    )
)

with_dims = (
    scored
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id").eqNullSafe(F.col("p.id")), "left")
    .select("s.*", F.col("p.stock_qty"))
)

enriched = (
    with_dims
    .select(
        "*",
        F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4).alias("quantity_share"),
        F.sum("stock_qty").over(w3).alias("cumulative_stock_qty"),
        F.dense_rank().over(w4).alias("running_avg_unit_price_dense_rank"),
        F.sum("running_sum_unit_price").over(w5).alias("running_running_sum_unit_price"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.shipping_fee"))
)

filtered = (
    joined
    .filter(F.col("running_sum_unit_price") < 1000)
    .select("*", F.coalesce("shipping_fee", F.lit(0)).alias("shipping_fee_filled"))
)

enriched_order_items = (
    filtered
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.running_min_unit_price",
        "a.running_avg_unit_price",
        "a.quantity_share",
        "a.cumulative_stock_qty",
        "a.running_avg_unit_price_dense_rank",
        "a.shipping_fee",
        F.col("d.product_name"),
        F.col("d.cost"),
    )
)

result = (
    enriched_order_items
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("s.quantity_share")).alias("min_quantity_share"),
        F.min(F.col("s.running_avg_unit_price_dense_rank")).alias("min_running_avg_unit_price_dense_rank"),
    )
    .orderBy(F.col("min_quantity_share").asc_nulls_first())
)
