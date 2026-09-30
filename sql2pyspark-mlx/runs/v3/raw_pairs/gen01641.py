from pyspark.sql import functions as F

valid_products = df_products.filter(F.col("cost") > 710)

summary = (
    valid_products
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"))
)

result = (
    summary
    .filter(F.col("stddev_stock_qty") < 25)
    .select("supplier_id", "stddev_stock_qty")
    .orderBy("supplier_id")
    .limit(3)
)
