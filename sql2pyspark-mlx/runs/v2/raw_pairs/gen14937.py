from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("price") < 1050)
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(F.col("s.country"), F.col("s.city"), F.col("pbs.stddev_cost"))
    .orderBy("stddev_cost")
)
