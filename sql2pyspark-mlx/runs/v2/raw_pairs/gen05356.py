from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.col("supplier_id") == 39) & ((F.col("category") == "Books") | F.col("product_name").like("De%")))
    .withColumn("sorted_tags", F.sort_array("tags"))
)

products_stats = (
    recent_products
    .groupBy(
        F.when(F.col("stock_qty") >= 406, "high").when(F.col("stock_qty") >= 160, "mid").otherwise("low").alias("stock_qty_bucket"),
        F.date_trunc("MONTH", "launch_date").alias("launch_month"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Home", F.col("stock_qty")).otherwise(0)).alias("home_stock_qty"),
    )
)

result = (
    products_stats
    .filter(F.col("home_stock_qty") >= 68)
    .orderBy(F.col("launch_month").desc())
)
