from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("stock_qty").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("stock_qty").desc(), "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isin([13, 18, 27]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
        F.round("unit_price", 2).alias("unit_price_rounded"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.product_id",
        "e.unit_price",
        "e.unit_price_rounded",
        F.col("b.cost"),
        F.col("b.stock_qty"),
    )
)

ranked = (
    with_dims
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "product_id", "unit_price", "stock_qty")
)

enriched = (
    deduped
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.cost") > 460)
    .select("s.id", "s.product_id", "s.unit_price", "s.stock_qty", F.col("p.brand"))
)

result = (
    enriched
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .groupingSets(
        [["category", "order_item_brand"], ["category"]],
        F.col("p.category").alias("category"),
        F.col("e.brand").alias("order_item_brand"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.unit_price")).alias("total_unit_price"),
        F.round(F.sum(F.col("e.stock_qty")), 2).alias("total_stock_qty"),
    )
)
