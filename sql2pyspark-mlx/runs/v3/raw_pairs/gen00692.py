from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") < 1160)
    .groupBy("category", F.month("launch_date").alias("launch_month"), "brand")
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.max("cost").alias("max_cost"),
    )
    .orderBy("brand", F.col("max_cost").desc())
)
