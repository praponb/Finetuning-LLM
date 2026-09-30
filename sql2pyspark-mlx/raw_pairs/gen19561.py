from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id")
w2 = Window.orderBy("price", "id")
w3 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")
w4 = Window.partitionBy("brand")
w5 = Window.partitionBy("supplier_id", "brand").orderBy(F.col("cost").desc_nulls_first())
w6 = Window.partitionBy("brand").orderBy(F.col("stock_qty_bucket").desc(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("brand") == "Initech"))
    .select("*", F.least("stock_qty", "cost").alias("least_stock_qty_cost"))
)

enriched = (
    filtered
    .select(
        "*",
        F.lag("stock_qty", 2).over(w).alias("prev_stock_qty"),
        F.ntile(5).over(w2).alias("price_bucket"),
        F.ntile(4).over(w3).alias("stock_qty_bucket"),
        F.count("stock_qty").over(w4).alias("brand_count_stock_qty"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "least_stock_qty_cost",
        "prev_stock_qty",
        "price_bucket",
        "stock_qty_bucket",
        "brand_count_stock_qty",
    )
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "supplier_id",
        "least_stock_qty_cost",
        "price_bucket",
        "stock_qty_bucket",
    )
)

with_latest = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

result = (
    with_latest
    .groupBy("brand", "supplier_id")
    .agg(
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("stock_qty_per_acme"),
    )
)
