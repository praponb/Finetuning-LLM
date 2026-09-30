from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.stddev_stock_qty") > 31)
    .select(
        F.col("suppliers.city"),
        F.col("suppliers.country"),
        F.col("t.stddev_stock_qty"),
        F.col("t.avg_cost"),
    )
)
