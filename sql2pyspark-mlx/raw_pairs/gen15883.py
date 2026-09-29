from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("brand")
w4 = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("brand").orderBy(F.col("price").desc_nulls_first(), "id")
w7 = Window.partitionBy("brand").orderBy("price", "id")
w8 = Window.partitionBy("brand").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("brand").orderBy(F.col("corr_stock_qty_price").desc(), "id")
w10 = Window.orderBy("price", "id")
w11 = Window.partitionBy("brand").orderBy(F.col("price").asc_nulls_first())

with_last = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.brand",
        "s.price",
        "s.stock_qty",
        "s.tags",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price"),
    )
)

with_windows = (
    with_last
    .select(
        "*",
        F.sum("order_item_quantity").over(w2).alias("cumulative_order_item_quantity"),
        F.corr("stock_qty", "price").over(w_grp).alias("corr_stock_qty_price"),
        F.sum("unit_price").over(w4).alias("running_unit_price"),
    )
)

with_last_products = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"), F.col("c.sale_date").alias("store_sale_sale_date"))
)

ranked = (
    with_last_products
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "brand",
        "price",
        "stock_qty",
        "corr_stock_qty_price",
        "running_unit_price",
        "units",
        "store_sale_sale_date",
    )
)

ranked_products = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

kept = (
    ranked_products
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([3, 5, 9, 10])), "left_anti")
)

metrics = (
    kept
    .select(
        "id",
        "price",
        "stock_qty",
        "corr_stock_qty_price",
        "running_unit_price",
        "units",
        "dedup_rank",
        F.sum("units").over(w8).alias("cumulative_units"),
        F.ntile(3).over(w9).alias("corr_stock_qty_price_bucket"),
        F.round((F.col("units") / F.sum("units").over(w_grp)), 4).alias("units_share"),
        F.lag("price", 1, 0).over(w10).alias("prev_price"),
        F.percent_rank().over(w11).alias("price_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "price",
        "corr_stock_qty_price",
        "running_unit_price",
        "units",
        "corr_stock_qty_price_bucket",
        "units_share",
        "prev_price",
        "price_percent_rank",
    )
    .orderBy(
        F.col("price_percent_rank").desc_nulls_first(),
        "corr_stock_qty_price_bucket",
    )
)
