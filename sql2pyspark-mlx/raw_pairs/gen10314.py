from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")
w2 = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-03-18").cast("date")) & (F.col("price") >= 930))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        F.lower("category").alias("category_lower"),
    )
)

cleaned = (
    filtered
    .filter(F.col("brand").isNotNull())
    .select("*", F.array_join("tags", "|").alias("tags_text"))
)

kept = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.unit_price") >= 240), "left_anti")
)

scored = (
    kept
    .select(
        "*",
        F.ntile(10).over(w).alias("price_bucket"),
        F.sum("stock_qty").over(w2).alias("running_stock_qty"),
    )
)

base = (
    scored
    .filter((~F.col("brand").eqNullSafe(F.lit("Acme"))) & F.col("cost").between(230, 520))
    .withColumn("brand_group", F.when(F.col("brand").isin(["Globex", "Acme"]), "Priority").otherwise("Other"))
)

result = (
    base
    .select(
        "id",
        "brand",
        "price",
        "category_lower",
        "tags_text",
        "price_bucket",
        "running_stock_qty",
        "brand_group",
    )
    .orderBy(F.col("price_bucket").desc_nulls_first())
)
