from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNull())
    .groupBy("brand", "category")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
    .orderBy("brand")
)
