from pyspark.sql import functions as F

quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.min("quantity").alias("min_order_item_quantity"))

remaining_keys = (
    df_order_items
    .select("order_id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") < 3)
        .select("order_id", "product_id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "unit_price", F.col("min_order_item_quantity"))
    .orderBy("unit_price", "min_order_item_quantity")
)
