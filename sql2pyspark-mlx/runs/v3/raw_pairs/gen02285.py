from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("category") == "Sports", F.col("stock_qty")).otherwise(0)).alias("sports_stock_qty"),
    )
    .filter(F.col("sports_stock_qty") < 27)
)
