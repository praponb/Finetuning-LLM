from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("stock_qty") < 209)
    .groupBy("supplier_id")
    .agg(F.countDistinct("price").alias("unique_categorys"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .filter(F.col("pbs.unique_categorys") >= 8)
    .select(F.col("s.country"), F.col("s.rating"), F.col("pbs.unique_categorys"))
    .orderBy(F.col("rating").desc(), "country")
)
