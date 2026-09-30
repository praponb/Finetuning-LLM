from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("brand") == "Initech")
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("price"), 2).alias("stddev_price"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(F.col("s.country"), F.col("ss.stddev_price"))
    .orderBy(F.col("stddev_price").desc())
)
