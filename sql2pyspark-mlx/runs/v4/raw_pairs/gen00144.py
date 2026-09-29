from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.year("launch_date") >= 2023)
    .groupBy(
        F.when(F.col("brand").isin(["Stark", "Acme"]), "core").otherwise("other").alias("brand_group"),
        "brand",
    )
    .agg(
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
    .orderBy("fashion_count", "brand_group")
)
