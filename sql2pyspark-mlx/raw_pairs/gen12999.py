from pyspark.sql import functions as F

units_by_product_id = df_store_sales.groupBy("product_id").agg(F.max("units").alias("max_store_sale_units"))

enriched = (
    df_order_items
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.shipping_fee"))
)

result = (
    enriched
    .join(units_by_product_id, on="product_id", how="left")
    .select("id", "order_id", "product_id", "unit_price", F.col("max_store_sale_units"))
)
