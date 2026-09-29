from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.max(F.col("ss.units")).alias("max_units"),
        F.count(F.col("ss.units")).alias("units_count"),
    )
    .orderBy("max_units")
)
