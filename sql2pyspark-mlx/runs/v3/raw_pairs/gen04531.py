from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("stock_qty").alias("unique_brands"),
        F.countDistinct("price").alias("unique_suppliers"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .filter(F.col("pbs.unique_brands") >= 6)
    .select(
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("pbs.unique_brands"),
        F.col("pbs.unique_suppliers"),
    )
)
