from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .filter(F.col("P.max_quantity") >= 230)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.category"),
        F.col("P.max_quantity"),
    )
    .orderBy(F.col("max_quantity").desc(), "id")
    .limit(5)
)
