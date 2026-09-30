from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "outer")
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
        F.max(F.col("ss.units")).alias("max_units"),
    )
    .orderBy(F.col("max_units").desc(), "stddev_units", "supplier_id")
    .limit(3)
)
