from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-05-05").cast("date"))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.substring("product_name", 1, 3).alias("product_name_prefix"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity") == 5), "left_anti")
)

kept_keys = (
    eligible
    .select("supplier_id")
    .subtract(
        df_products
        .filter(F.col("price") < 330)
        .select("supplier_id")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("category").rlike("^(Books|Home)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("category_zone"),
    )
    .orderBy(F.col("stock_qty").desc(), F.col("brand").desc())
)
