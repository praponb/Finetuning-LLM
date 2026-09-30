from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("store_id").isin([2, 4, 6]))
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .filter(F.col("pt.max_quantity") > 90)
    .select(
        F.col("p.launch_date"),
        F.col("p.category"),
        F.col("pt.min_quantity"),
        F.col("pt.max_quantity"),
    )
    .orderBy(F.col("max_quantity").desc(), F.col("launch_date").desc())
)
