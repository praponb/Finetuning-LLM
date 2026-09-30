from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").isNull() & ((F.upper(F.trim("product_name")) == "COFFEE MAKER") | (F.col("launch_date") > "2023-06-01")))
    .agg(
        F.count("cost").alias("cost_count"),
        F.min("price").alias("min_price"),
        F.count("*").alias("num_products"),
    )
    .orderBy("min_price")
)
