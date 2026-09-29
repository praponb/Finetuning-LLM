from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.supplier_id").isNotNull())
    .select(
        F.col("p.brand"),
        F.col("oi.order_id"),
        F.concat_ws(",", F.col("p.product_name"), F.col("p.category")).alias("product_name_category_key"),
    )
)
