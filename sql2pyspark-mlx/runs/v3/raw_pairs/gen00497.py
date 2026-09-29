from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([4, 6, 8]) | (F.col("cost") >= 660))
    .groupBy(
        F.quarter("launch_date").alias("launch_quarter"),
        F.when(F.col("category").isin(["Grocery", "Fashion"]), "group_a").otherwise("other").alias("category_group"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
    .orderBy(F.col("launch_quarter").desc(), "category_group")
    .limit(10)
)
