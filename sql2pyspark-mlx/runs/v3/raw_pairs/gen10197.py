from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.brand"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.min(F.col("ss.units")).alias("min_units"),
    )
    .filter(F.col("store_sale_count") > 2)
    .orderBy(F.col("store_sale_count").desc())
)
