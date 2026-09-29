from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy(F.year("launch_date"), "supplier_id").orderBy("running_min_quantity", "id")
w4 = Window.partitionBy("brand")
w5 = Window.partitionBy("brand", "supplier_id")
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("price").alias("max_product_price"))

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("launch_date").between("2023-08-01", "2023-08-31"))
    .select(
        "*",
        (F.col("stock_qty") + F.col("price")).alias("stock_qty_plus_price"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.quantity") > 72), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.brand",
        "s.cost",
        "s.stock_qty",
        "s.launch_date",
        "s.supplier_id",
        "s.stock_qty_plus_price",
        "s.brand_filled",
        F.col("c.unit_price"),
        F.col("c.quantity"),
    )
)

with_windows = (
    with_latest
    .select(
        "*",
        F.avg("quantity").over(w_ordered).alias("running_avg_quantity"),
        F.min("quantity").over(w_ordered).alias("running_min_quantity"),
        F.count("quantity").over(w_ordered).alias("running_count_quantity"),
    )
)

scored = (
    with_windows
    .withColumn("prev_running_avg_quantity", F.lag("running_avg_quantity", 1).over(w_hist))
    .withColumn("corr_cost_stock_qty", F.corr("cost", "stock_qty").over(w4))
    .withColumn("brand_max_stock_qty", F.max("stock_qty").over(w5))
)

eligible_products = (
    scored
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.units") > 26), "left_semi")
)

result = (
    eligible_products
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "brand_filled",
        "unit_price",
        "quantity",
        F.col("max_product_price"),
    )
    .orderBy("quantity", F.col("cost").desc())
)
