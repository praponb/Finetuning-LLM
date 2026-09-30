from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("sale_date") >= "2025-07-12")
    .groupBy("product_id")
    .agg(
        F.countDistinct("units").alias("unique_products"),
        F.countDistinct("revenue").alias("unique_stores"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("pt.unique_products"),
        F.col("pt.unique_stores"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
