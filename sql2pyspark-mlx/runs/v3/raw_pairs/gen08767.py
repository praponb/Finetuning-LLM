from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .filter(F.col("pt.min_quantity") > 7.7)
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("pt.unique_products"),
        F.col("pt.min_quantity"),
    )
)
