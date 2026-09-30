from pyspark.sql import functions as F

unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.avg("unit_price").alias("avg_order_item_unit_price"))

matched_keys = (
    df_order_items
    .select("order_id", "id")
    .exceptAll(
        df_order_items
        .filter(F.col("unit_price") < 300)
        .select("order_id", "id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select("id", "product_id", "unit_price", F.col("avg_order_item_unit_price"))
    .orderBy(F.col("id").desc_nulls_last())
)
