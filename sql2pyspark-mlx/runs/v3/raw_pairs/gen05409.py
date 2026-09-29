from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("product_id") == 2)
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .filter(F.col("P.total_quantity") > 1240)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("P.avg_quantity"),
        F.col("P.total_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc(), "launch_date")
)
