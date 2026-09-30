from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("stock_qty") < 361)
    .groupBy("supplier_id")
    .agg(F.min("cost").alias("min_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .select(F.col("s.country"), F.col("pbs.min_cost"))
    .orderBy(F.col("min_cost").desc(), "country")
)
