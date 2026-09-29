from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.avg("price").alias("avg_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .filter(F.col("pbs.stddev_stock_qty") > 24)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.city"),
        F.col("pbs.stddev_stock_qty"),
        F.col("pbs.avg_price"),
    )
    .orderBy(F.col("stddev_stock_qty").desc())
)
