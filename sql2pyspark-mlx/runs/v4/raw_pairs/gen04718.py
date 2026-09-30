from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 5]))
    .groupBy("product_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .filter(F.col("pt.total_count") > 9)
    .select(F.col("p.supplier_id"), F.col("p.launch_date"), F.col("pt.total_count"))
    .orderBy(F.col("total_count").desc())
)
