from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_orders"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .select(F.col("p.cost"), F.col("pt.unique_orders"), F.col("pt.avg_unit_price"))
    .orderBy("avg_unit_price")
)
