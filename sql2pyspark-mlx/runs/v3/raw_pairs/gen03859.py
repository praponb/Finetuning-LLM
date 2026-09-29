from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNull() & F.col("stock_qty").between(266, 381))
    .groupBy("category", "brand")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.sum("price").alias("total_price"),
    )
    .filter(F.col("total_price") > 6300)
    .orderBy("min_launch_date")
)
