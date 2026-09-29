from pyspark.sql import functions as F

enriched = (
    df_order_items
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.price"), F.col("b.brand"), F.col("b.launch_date"))
)

result = (
    enriched
    .select("id", "order_id", "unit_price", "price", "brand")
    .orderBy(F.col("brand").desc_nulls_last())
)
