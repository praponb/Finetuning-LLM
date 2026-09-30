from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.max("price").alias("max_price"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.stddev_stock_qty") > 31)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("t.stddev_stock_qty"),
        F.col("t.max_price"),
    )
    .orderBy("id")
    .limit(5)
)
