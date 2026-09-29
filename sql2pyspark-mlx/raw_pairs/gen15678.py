from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy("brand")
w4 = Window.partitionBy("brand").orderBy(F.col("category_count_stock_qty_filled_plus_cost").desc())
w_part = Window.partitionBy("brand").orderBy(F.col("stock_qty_filled").desc())

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_clean"),
    )
)

matched_keys = (
    base
    .select("id", "supplier_id")
    .exceptAll(
        df_products
        .filter(F.col("category") == "Electronics")
        .select("id", "supplier_id")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

base_products = (
    verified
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-06-16"))) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "stock_qty_filled",
        "brand_clean",
        F.size("tags").alias("tags_count"),
        (F.col("stock_qty_filled") + F.col("cost")).alias("stock_qty_filled_plus_cost"),
    )
)

enriched = (
    base_products
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty_filled",
        "tags_count",
        F.count("stock_qty_filled_plus_cost").over(w).alias("category_count_stock_qty_filled_plus_cost"),
        F.percent_rank().over(w2).alias("cost_percent_rank"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity") == 6), "left_anti")
)

result = (
    eligible
    .filter(F.col("category_count_stock_qty_filled_plus_cost").between(8, 10))
    .select(
        "product_name",
        "stock_qty_filled",
        "cost_percent_rank",
        F.max("price").over(w3).alias("brand_max_price"),
        F.dense_rank().over(w4).alias("category_count_stock_qty_filled_plus_cost_dense_rank"),
        F.dense_rank().over(w_part).alias("stock_qty_filled_dense_rank"),
    )
)
