from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.rating") < 4.0)
    .groupBy(F.col("s.country"))
    .agg(F.round(F.stddev(F.col("p.stock_qty")), 2).alias("stddev_stock_qty"))
    .orderBy("stddev_stock_qty", "country")
    .limit(10)
)
