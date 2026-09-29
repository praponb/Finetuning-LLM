from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") >= 217)
    .groupBy(
        F.when(F.col("stock_qty") >= 359, "high").when(F.col("stock_qty") >= 69, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.max("price").alias("max_price"),
        F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0)).alias("sports_count"),
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("stock_qty")).otherwise(0)).alias("umbrella_stock_qty"),
    )
    .filter(F.col("umbrella_stock_qty") > 62)
    .orderBy(F.col("umbrella_stock_qty").desc(), "stock_qty_bucket")
    .limit(5)
)
