from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("price") > 510)
    .groupBy("supplier_id")
    .agg(
        F.max("price").alias("max_price"),
        F.countDistinct("price").alias("unique_brands"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("ProductsBySupplier"), F.col("suppliers.id") == F.col("ProductsBySupplier.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("suppliers.city"),
        F.col("ProductsBySupplier.max_price"),
        F.col("ProductsBySupplier.unique_brands"),
    )
    .orderBy(F.col("city").desc(), F.col("unique_brands").desc())
)
