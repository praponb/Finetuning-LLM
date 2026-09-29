from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.round(F.stddev(F.col("p.price")), 2).alias("stddev_price"),
        F.avg(F.col("p.stock_qty")).alias("avg_stock_qty"),
    )
)
