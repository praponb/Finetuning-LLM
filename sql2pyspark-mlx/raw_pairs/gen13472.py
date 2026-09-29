from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .groupBy("brand")
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.sum("price").alias("total_price"),
    )
    .filter(F.col("min_stock_qty") > 280)
    .orderBy("total_price")
)
