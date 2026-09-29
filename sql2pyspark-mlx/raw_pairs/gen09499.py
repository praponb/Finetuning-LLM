from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.sum("price").alias("total_price"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
    .filter(F.col("max_stock_qty") < 340)
    .orderBy(F.col("brand").desc(), F.col("max_stock_qty").desc())
)
