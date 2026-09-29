from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
    .filter(F.col("total_stock_qty") < 700)
    .drop("total_stock_qty")
    .orderBy("brand")
    .limit(20)
)
