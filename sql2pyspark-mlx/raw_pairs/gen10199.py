from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("category", "order_id").orderBy(F.col("unit_price").desc(), "id")
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id").orderBy(F.col("greatest_quantity_unit_price").desc_nulls_last())
w5 = Window.partitionBy("product_category", "order_id").orderBy(F.col("stock_qty").desc_nulls_first(), "id")
w6 = Window.partitionBy("category").orderBy("stock_qty", "id")
w7 = Window.partitionBy("order_id").orderBy(F.col("greatest_quantity_unit_price_cume_dist").desc_nulls_last())
w8 = Window.partitionBy("category")

cleaned = (
    df_order_items
    .filter(F.col("order_id").isin([34, 36]))
    .select(
        "*",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)

joined = (
    cleaned
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.category") == "Home")
    .select(
        "e.id",
        "e.order_id",
        "e.product_id",
        "e.quantity",
        "e.unit_price",
        "e.greatest_quantity_unit_price",
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("p.category"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "order_id",
        "product_id",
        "stock_qty",
        "category",
        F.round((F.col("unit_price") / F.sum("unit_price").over(w)), 4).alias("unit_price_share"),
        F.ntile(4).over(w2).alias("unit_price_bucket"),
        F.min("quantity").over(w3).alias("order_id_min_quantity"),
        F.cume_dist().over(w4).alias("greatest_quantity_unit_price_cume_dist"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share_2"),
    )
)

enriched = (
    with_windows
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.launch_date"),
        F.col("p.category").alias("product_category"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .select(
        "id",
        "product_id",
        "stock_qty",
        "unit_price_bucket",
        "greatest_quantity_unit_price_cume_dist",
        "unit_price_share_2",
        F.lag("unit_price_bucket", 1).over(w6).alias("prev_unit_price_bucket"),
        F.rank().over(w7).alias("greatest_quantity_unit_price_cume_dist_rank"),
        F.avg("unit_price_share").over(w8).alias("category_avg_unit_price_share"),
    )
)
