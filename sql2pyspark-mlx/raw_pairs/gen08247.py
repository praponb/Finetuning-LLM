from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.cost") < 630)
    .groupBy(F.col("p.category"))
    .agg(F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"))
    .filter(F.col("stddev_revenue") > 49)
)
