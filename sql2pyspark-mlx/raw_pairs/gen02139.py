from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("stock_qty")).otherwise(0)).alias("initech_stock_qty"),
    )
    .orderBy(F.col("avg_stock_qty").desc())
)
