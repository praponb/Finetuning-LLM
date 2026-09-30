from pyspark.sql import functions as F

unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

matched_keys = (
    df_inventory
    .select("store_id", "product_id")
    .exceptAll(
        df_inventory
        .filter(F.col("quantity").between(130, 187))
        .select("store_id", "product_id")
    )
)

reconciled = (
    df_inventory
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.col("max_order_item_unit_price"),
    )
)
