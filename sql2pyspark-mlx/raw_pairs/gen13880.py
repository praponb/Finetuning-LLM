from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand").orderBy(F.col("price").desc(), "id")
w_recent = Window.partitionBy("brand").orderBy("stock_qty", "id")

filtered = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue").alias("store_sale_revenue"), F.col("c.sale_date"))
)

deduped = (
    latest_match
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "brand", "price", "stock_qty", "tags")
)

enriched = (
    deduped
    .filter(F.array_contains("tags", "bestseller"))
    .select(
        "id",
        "tags",
        F.lead("stock_qty", 1, 0).over(w_recent).alias("next_stock_qty"),
        F.lead("stock_qty", 2, 0).over(w_recent).alias("next_stock_qty_2"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([11, 18, 26])), "left_anti")
)

result = (
    not_flagged
    .select("id", "tags", "next_stock_qty", "next_stock_qty_2")
    .orderBy(F.col("id").asc_nulls_last())
)
