from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("product_id").isin([1, 15]))
    .groupBy("product_id")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("pt.min_unit_price"),
        F.col("pt.stddev_quantity"),
    )
    .orderBy(F.col("min_unit_price").desc())
)
