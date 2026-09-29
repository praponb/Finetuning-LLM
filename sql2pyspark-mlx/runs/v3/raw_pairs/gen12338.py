from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.countDistinct("revenue").alias("unique_stores"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .filter(F.col("pt.unique_stores") >= 8)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("pt.units_range"),
        F.col("pt.unique_stores"),
    )
    .orderBy(F.col("units_range").desc())
)
