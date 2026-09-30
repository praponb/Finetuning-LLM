from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.length("product_name") < 13)
    .groupBy("supplier_id")
    .agg(
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .filter(F.col("pbs.stddev_stock_qty") >= 66)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("pbs.acme_count"),
        F.col("pbs.stddev_stock_qty"),
    )
)
