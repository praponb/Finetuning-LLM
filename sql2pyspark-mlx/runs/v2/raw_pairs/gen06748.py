from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("price"), 2).alias("stddev_price"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .filter(F.col("pbs.stddev_price") >= 45)
    .select(F.col("s.city"), F.col("s.rating"), F.col("pbs.stddev_price"))
)
