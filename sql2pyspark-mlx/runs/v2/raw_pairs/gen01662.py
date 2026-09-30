from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Globex")
    .groupBy("supplier_id")
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
    )
    .orderBy(F.col("stddev_price").desc())
)
