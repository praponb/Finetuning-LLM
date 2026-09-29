from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("quantity") > 7)
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.max("unit_price").alias("max_unit_price"),
    )
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("ProductTotals"), F.col("products.id") == F.col("ProductTotals.product_id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.supplier_id"),
        F.col("ProductTotals.stddev_unit_price"),
        F.col("ProductTotals.max_unit_price"),
    )
    .orderBy(F.col("id").desc())
)
