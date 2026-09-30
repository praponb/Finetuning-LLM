from pyspark.sql import functions as F

quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

remaining_keys = (
    df_order_items
    .select("product_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("order_id") == 6)
        .select("product_id", "id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.col("sum_order_item_quantity"),
    )
    .orderBy(F.col("quantity").desc_nulls_first())
)
