from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("last_restock").between("2024-04-15", "2024-06-01"))
    .groupBy("product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(F.col("p.price"), F.col("p.category"), F.col("ps.stddev_quantity"))
    .orderBy(F.col("stddev_quantity").desc())
)
