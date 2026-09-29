from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("category").isin(["Electronics", "Sports"]))
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(F.col("s.rating"), F.col("pbs.stddev_stock_qty"))
    .orderBy(F.col("stddev_stock_qty").desc())
)
