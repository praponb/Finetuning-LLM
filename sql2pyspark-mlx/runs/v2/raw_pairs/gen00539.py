from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("stock_qty") == 398)
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("products_by_supplier"), F.col("suppliers.id") == F.col("products_by_supplier.supplier_id"), "inner")
    .filter(F.col("products_by_supplier.umbrella_count") > 79)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("products_by_supplier.umbrella_count"),
    )
)
