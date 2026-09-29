from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0)).alias("stark_count"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("products_by_supplier"), F.col("suppliers.id") == F.col("products_by_supplier.supplier_id"), "inner")
    .filter(F.col("products_by_supplier.stark_count") >= 71)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
        F.col("products_by_supplier.stark_count"),
    )
    .orderBy("rating", F.col("id").desc())
)
