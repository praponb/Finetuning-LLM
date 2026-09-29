from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("supplier_id").isNull()).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .select(F.col("ss.id"), F.col("ss.units"))
    .orderBy(F.col("units").desc(), F.col("id").desc())
)
