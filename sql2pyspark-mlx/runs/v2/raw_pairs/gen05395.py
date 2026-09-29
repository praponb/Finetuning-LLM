from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
        F.max("launch_date").alias("last_launch_date"),
    )
    .orderBy("books_count")
)
