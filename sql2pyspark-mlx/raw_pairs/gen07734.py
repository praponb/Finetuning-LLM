from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([2, 4, 11]) & F.col("category").isin(["Grocery", "Home", "Books"]) & (F.length("product_name") < 5))
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.countDistinct("cost").alias("unique_brands"),
    )
    .filter(F.col("unique_brands") > 1)
    .drop("unique_brands")
    .orderBy("total_stock_qty", "launch_quarter")
    .limit(5)
)
