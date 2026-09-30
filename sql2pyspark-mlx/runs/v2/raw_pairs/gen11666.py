from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.countDistinct("cost").alias("unique_suppliers"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("P"), F.col("s.id") == F.col("P.supplier_id"), "inner")
    .filter(F.col("P.unique_suppliers") > 9)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.country"),
        F.col("P.unique_suppliers"),
    )
    .orderBy("country")
)
