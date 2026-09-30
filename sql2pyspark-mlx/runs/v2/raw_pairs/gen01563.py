from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-06-08", "2023-09-18"))
    .groupBy("product_id")
    .agg(
        F.count("units").alias("units_count"),
        F.min("sale_date").alias("min_sale_date"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("p.cost"),
        F.col("p.category"),
        F.col("t.units_count"),
        F.col("t.min_sale_date"),
    )
    .orderBy(F.col("units_count").desc())
)
