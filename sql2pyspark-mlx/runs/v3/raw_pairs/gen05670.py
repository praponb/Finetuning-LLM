from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .groupBy(F.col("p.id"), F.col("p.brand"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        (F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"),
    )
    .filter(F.col("units_range") >= 71)
)
