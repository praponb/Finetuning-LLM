from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("order_id") == 32)
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

kept_keys = (
    base
    .select("order_id")
    .intersect(
        df_order_items
        .filter(F.col("unit_price") >= 340)
        .select("order_id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("quantity").between(6, 8))
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity_filled",
        (F.col("quantity") + F.col("quantity_filled")).alias("quantity_plus_quantity_filled"),
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)

with_dims = (
    filtered
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.unit_price",
        "a.quantity_filled",
        F.col("p.stock_qty"),
    )
)

joined = (
    with_dims
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select(
        "e.*",
        F.col("b.product_name"),
        F.col("b.stock_qty").alias("product_stock_qty"),
    )
)

result = (
    joined
    .select(
        "id",
        "unit_price",
        "stock_qty",
        "product_name",
        "product_stock_qty",
        F.when(F.col("product_name").rlike("^(Smart Watch|Coffee Maker)$"), "GROUP_A").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "PRIORITY").otherwise("CORE").alias("product_name_zone"),
    )
    .orderBy(F.col("stock_qty").desc())
)
