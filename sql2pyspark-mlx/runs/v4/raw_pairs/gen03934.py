from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("launch_date").asc_nulls_last())
w2 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")
w3 = Window.partitionBy("supplier_id").orderBy(F.col("price").desc_nulls_last())
w4 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc())
w5 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("category").orderBy("stock_qty", "id")
w7 = Window.partitionBy("brand", "supplier_id").orderBy(F.col("price").desc_nulls_first())
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

kept = (
    df_products
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") >= F.col("s.launch_date")), "left_semi")
)

ranked = (
    kept
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows = (
    ranked
    .withColumn("stock_qty_bucket", F.ntile(3).over(w2))
    .withColumn("price_dense_rank", F.dense_rank().over(w3))
    .withColumn("stock_qty_percent_rank", F.percent_rank().over(w4))
)

scored = (
    with_windows
    .select(
        "*",
        F.last("brand", ignorenulls=True).over(w5).alias("last_known_brand"),
        F.ntile(4).over(w6).alias("stock_qty_bucket_2"),
        F.cume_dist().over(w7).alias("price_cume_dist"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"), F.col("c.units"))
)

valid = (
    with_last
    .filter(F.col("launch_date") >= F.lit("2023-07-16").cast("date"))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "stock_qty_bucket",
        "price_dense_rank",
        "stock_qty_percent_rank",
        "last_known_brand",
        "stock_qty_bucket_2",
        "revenue",
        "units",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)

result = (
    valid
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "stock_qty_bucket",
        "stock_qty_percent_rank",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
