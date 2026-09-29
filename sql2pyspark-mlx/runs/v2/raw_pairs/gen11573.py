from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .select(
        F.col("p.cost"),
        F.col("p.category"),
        F.col("pt.min_quantity"),
        F.col("pt.stddev_quantity"),
    )
    .orderBy("stddev_quantity")
)
