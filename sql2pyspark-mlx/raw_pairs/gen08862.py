from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("stock_qty", "id")
w2 = Window.partitionBy("city").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
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
        (F.col("cost") / F.nullif("stock_qty", F.lit(0))).alias("cost_per_stock_qty"),
        F.round((F.col("cost") * 0.85), 2).alias("cost_scaled"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "left")
    .filter(F.col("p.country") == "DE")
    .select(
        "e.id",
        "e.product_name",
        "e.brand",
        "e.price",
        "e.cost",
        "e.stock_qty",
        "e.tags",
        "e.supplier_id",
        "e.cost_scaled",
        "e.brand_filled",
        F.col("p.city"),
        F.col("p.country"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.unit_price").between(150, 230)), "left_semi")
)

valid = (
    not_flagged
    .filter(F.col("brand").isNotNull() & F.col("country").isNull())
    .select(
        "*",
        F.array_join("tags", ", ").alias("tags_text"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_filled_2"),
    )
)

result = (
    valid
    .select(
        "id",
        "product_name",
        "price",
        "tags",
        "supplier_id",
        "city",
        "country",
        F.ntile(3).over(w).alias("stock_qty_bucket"),
        F.last("cost_scaled", ignorenulls=True).over(w2).alias("last_known_cost_scaled"),
    )
)
