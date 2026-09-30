from pyspark.sql import functions as F

unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

enriched = (
    df_order_items
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.quantity",
        F.col("b.price"),
    )
)

result = (
    enriched
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "quantity", "price", F.col("max_order_item_unit_price"))
    .orderBy(F.col("max_order_item_unit_price").desc(), F.col("price").desc())
)
