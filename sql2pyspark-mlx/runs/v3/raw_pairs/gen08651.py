from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("unit_price") < 270)
    .groupBy("product_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(F.col("p.product_name"), F.col("t.avg_unit_price"))
    .orderBy(F.col("avg_unit_price").desc())
)
