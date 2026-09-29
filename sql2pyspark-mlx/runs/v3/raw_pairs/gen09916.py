from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.countDistinct("units").alias("unique_stores"),
        F.sum("units").alias("total_units"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(F.col("p.cost"), F.col("ps.unique_stores"), F.col("ps.total_units"))
    .orderBy(F.col("total_units").desc())
)
