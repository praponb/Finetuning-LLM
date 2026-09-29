from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.countDistinct("price").alias("unique_suppliers"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("products_by_supplier"), F.col("suppliers.id") == F.col("products_by_supplier.supplier_id"), "inner")
    .filter(F.col("products_by_supplier.unique_suppliers") >= 5)
    .select(
        F.col("suppliers.city"),
        F.col("products_by_supplier.first_launch_date"),
        F.col("products_by_supplier.unique_suppliers"),
    )
    .orderBy("first_launch_date")
)
