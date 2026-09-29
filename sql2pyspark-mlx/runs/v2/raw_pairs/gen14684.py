from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") <= "2024-07-16")
    .groupBy("brand")
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.count("*").alias("num_products"),
    )
    .filter(F.col("min_stock_qty") > 290)
    .select("brand", "min_stock_qty", "num_products")
    .orderBy("num_products", "brand")
    .limit(5)
)
