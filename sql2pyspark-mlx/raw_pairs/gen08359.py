from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") <= 3700) & (F.col("revenue") >= 3800) & F.col("store_id").isin([4, 5]))
    .groupBy("product_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.count("*").alias("num_store_sales"),
    )
    .filter(F.col("num_store_sales") > 4)
    .orderBy("first_sale_date", "product_id")
    .limit(10)
)
