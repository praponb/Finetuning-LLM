from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-06-16").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.col("stock_qty").cast("string").alias("stock_qty_str"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

result = (
    base
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([25, 26, 35, 36])), "left_semi")
)
