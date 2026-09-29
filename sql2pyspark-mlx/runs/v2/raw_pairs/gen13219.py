from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("products_by_supplier"), F.col("suppliers.id") == F.col("products_by_supplier.supplier_id"), "inner")
    .select(
        F.col("suppliers.rating"),
        F.col("products_by_supplier.brand_values"),
        F.col("products_by_supplier.total_stock_qty"),
    )
    .orderBy(F.col("rating").desc())
)
