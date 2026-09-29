from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.brand"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        (F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"),
    )
    .orderBy("units_range", F.col("brand").desc())
)
