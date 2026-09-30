from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("unit_price") <= 290.4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
    )
)

screened = (
    filtered_moves.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.category") != "Toys"), "left_semi")
)

joined = (
    screened.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.col("w.opened_on").alias("warehouse_opened_on"))
)

enriched = (
    joined
    .select(
        "id",
        "warehouse_id",
        "qty",
        "warehouse_opened_on",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

result = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "qty",
        "warehouse_opened_on",
        "unit_price_bucket",
        F.date_format(F.col("warehouse_opened_on"), "yyyy-MM-dd").alias("warehouse_opened_on_label"),
    )
    .orderBy(F.col("warehouse_opened_on_label").desc_nulls_first())
)
