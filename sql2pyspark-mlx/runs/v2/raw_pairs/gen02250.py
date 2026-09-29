from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .filter(F.col("P.stddev_quantity") > 61)
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("P.stddev_quantity"),
        F.col("P.avg_quantity"),
    )
)
