from pyspark.sql import functions as F

units_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("units").alias("avg_store_sale_units"))

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("quantity") - F.col("unit_price")))
)

matched_keys = (
    cleaned
    .select("product_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") == 6)
        .select("product_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select(
        "s.product_id",
        "s.quantity",
        "s.unit_price",
        "s.line_total",
        F.col("b.product_name"),
        F.col("b.launch_date"),
        F.col("b.brand"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.product_id",
        "s.quantity",
        "s.product_name",
        "s.launch_date",
        F.col("p.stock_qty"),
    )
)

result = (
    joined
    .join(units_by_product_id, on="product_id", how="left")
    .select("product_id", "quantity", "launch_date", F.col("avg_store_sale_units"))
    .orderBy(F.col("launch_date").desc())
)
