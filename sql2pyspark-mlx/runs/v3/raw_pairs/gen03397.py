from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("cost").alias("unique_brands"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("pbs.unique_brands"),
        F.col("pbs.min_stock_qty"),
    )
    .orderBy(F.col("rating").desc(), "unique_brands")
)
