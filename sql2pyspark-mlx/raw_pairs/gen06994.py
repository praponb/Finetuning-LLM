from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w3 = Window.orderBy("units", "id")
w4 = Window.partitionBy("brand").orderBy(F.col("quantity").desc(), "id")

cleaned = (
    df_products
    .filter((F.length("product_name") < 13) & (F.col("launch_date") > F.lit("2023-03-22").cast("date")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.when(F.col("category") == "Fashion", 1).otherwise(0).alias("is_fashion"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.inventory_last_restock")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.brand",
        "s.price",
        "s.stock_qty",
        "s.supplier_id",
        "s.is_fashion",
        "s.quantity",
        "s.inventory_last_restock",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.units"),
    )
)

enriched = (
    with_last
    .select(
        "id",
        "price",
        "stock_qty",
        "quantity",
        "inventory_last_restock",
        "units",
        F.ntile(5).over(w3).alias("units_bucket"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id") == 32), "left_anti")
)

result = (
    eligible
    .groupBy(F.when(F.col("price") >= 940, "HIGH").otherwise("LOW").alias("price_tier"))
    .agg(
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("units_bucket"), 2).alias("avg_units_bucket"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
    .orderBy(F.col("quantity_p95").desc())
)
