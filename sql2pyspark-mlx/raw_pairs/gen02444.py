from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
        F.countDistinct(F.col("ss.units")).alias("unique_stores"),
    )
    .orderBy(F.col("supplier_id").desc(), F.col("stddev_units").desc())
)
