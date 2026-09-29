from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.orderBy("launch_date", "id").rowsBetween(-2, -1)
w3 = Window.orderBy(F.col("inventory_quantity").desc(), "id")
w4 = Window.orderBy("stock_qty_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("inventory_quantity_bucket", "id").rowsBetween(-2, -1)

with_last = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.price",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

base = (
    with_last
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "price",
        "stock_qty",
        "launch_date",
        "inventory_quantity",
        F.date_format("launch_date", "yyyy-MM-dd").alias("launch_date_label"),
    )
)

scored = (
    base
    .filter(F.col("launch_date") > "2024-10-13")
    .select(
        "id",
        "price",
        F.avg("stock_qty").over(w2).alias("stock_qty_avg_2_prior"),
        F.ntile(10).over(w3).alias("inventory_quantity_bucket"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([3, 4, 8, 10])), "left_anti")
)

metrics = (
    kept
    .select(
        "*",
        F.last("inventory_quantity_bucket", ignorenulls=True).over(w4).alias("last_known_inventory_quantity_bucket"),
        F.avg("stock_qty_avg_2_prior").over(w5).alias("stock_qty_avg_2_prior_avg_2_prior"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("inventory_quantity_bucket") >= 7, "HIGH").otherwise("LOW").alias("inventory_quantity_bucket_tier"),
        F.when(F.col("last_known_inventory_quantity_bucket") >= 7, "LARGE").otherwise("SMALL").alias("last_known_inventory_quantity_bucket_tier"),
    )
    .agg(
        F.round(F.avg("last_known_inventory_quantity_bucket"), 2).alias("avg_last_known_inventory_quantity_bucket"),
        F.sum("stock_qty_avg_2_prior").alias("total_stock_qty_avg_2_prior"),
        F.percentile_approx("stock_qty_avg_2_prior", 0.5).alias("stock_qty_avg_2_prior_p50"),
        F.min("stock_qty_avg_2_prior").alias("min_stock_qty_avg_2_prior"),
    )
)
