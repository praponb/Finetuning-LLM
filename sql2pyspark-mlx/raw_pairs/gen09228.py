from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("greatest_unit_price_quantity").desc_nulls_last(), "id")

cleaned = (
    df_order_items
    .filter(F.col("unit_price") > 150)
    .select(
        "*",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
    )
)

cleaned_order_items = (
    cleaned
    .filter(F.col("product_id").isin([19, 30]))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "greatest_unit_price_quantity",
        F.round("unit_price", 2).alias("unit_price_rounded"),
    )
)

joined = (
    cleaned_order_items
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id").eqNullSafe(F.col("d.id")), "inner")
    .filter(F.col("d.price").between(820, 1140))
    .select(
        "e.id",
        "e.product_id",
        "e.greatest_unit_price_quantity",
        F.col("d.cost"),
        F.col("d.stock_qty"),
        F.col("d.launch_date"),
    )
)

latest = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_dims = (
    latest
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "src.id",
        "src.product_id",
        "src.cost",
        "src.stock_qty",
        F.col("p.brand"),
        F.col("p.stock_qty").alias("product_stock_qty"),
    )
)

result = (
    with_dims
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["category", "order_item_brand"], ["category"], []],
        F.col("d.category").alias("category"),
        F.col("a.brand").alias("order_item_brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.product_stock_qty")).alias("max_product_stock_qty"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy("max_product_stock_qty")
)
