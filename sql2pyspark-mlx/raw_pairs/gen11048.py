from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "supplier_id").orderBy("cost", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy(F.col("last4_min_stock_qty").desc(), "id")
w3 = Window.partitionBy("supplier_id").orderBy(F.col("last4_max_stock_qty").desc_nulls_first())
w_recent = Window.orderBy(F.col("rating").desc_nulls_first())
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("price").alias("min_product_price"))

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("launch_date") > F.lit("2023-08-24").cast("date")))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.initcap(F.trim("product_name")).alias("product_name_title"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.city").isin(["Bangkok", "London"]))
    .select(
        "e.id",
        "e.category",
        "e.price",
        "e.cost",
        "e.stock_qty",
        "e.supplier_id",
        "e.product_name_title",
        F.col("b.city"),
        F.col("b.rating"),
    )
)

enriched = (
    with_dims
    .select(
        "id",
        "supplier_id",
        "city",
        "rating",
        F.min("stock_qty").over(w).alias("last4_min_stock_qty"),
        F.count("stock_qty").over(w).alias("last4_count_stock_qty"),
        F.max("stock_qty").over(w).alias("last4_max_stock_qty"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "supplier_id", "city", "rating", "last4_max_stock_qty")
)

enriched_products = (
    latest
    .select(
        "*",
        F.cume_dist().over(w3).alias("last4_max_stock_qty_cume_dist"),
        F.cume_dist().over(w_recent).alias("rating_cume_dist"),
    )
)

result = (
    enriched_products
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "supplier_id",
        "city",
        "rating",
        "last4_max_stock_qty_cume_dist",
        F.col("min_product_price"),
    )
    .orderBy(F.col("min_product_price").desc_nulls_first())
)
