from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.year("launch_date") == 2024) | (F.col("price") > 570))
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("category") == "Sports", F.col("stock_qty")).otherwise(0)).alias("sports_stock_qty"),
    )
)
