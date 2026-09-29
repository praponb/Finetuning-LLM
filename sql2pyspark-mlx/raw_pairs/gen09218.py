from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(F.col("suppliers.country"))
    .agg(
        F.count(F.col("products.stock_qty")).alias("stock_qty_count"),
        F.round(F.stddev(F.col("products.cost")), 2).alias("stddev_cost"),
    )
    .filter(F.col("stock_qty_count") >= 5)
)
