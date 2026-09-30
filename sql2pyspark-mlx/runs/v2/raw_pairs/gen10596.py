from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"))
    .orderBy(F.col("stddev_units").desc(), "brand")
    .limit(1)
)
