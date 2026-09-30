from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("p.stock_qty") < 150) | (F.col("ss.units") <= 22))
    .select(F.col("ss.units"), F.col("p.brand"))
    .orderBy(F.col("units").desc())
)
