from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2023-04-01", "2023-12-01") & (F.col("price") >= 700) & F.month("launch_date").isin([5, 6, 11]))
    .select("supplier_id", "stock_qty", F.ceil("price").alias("price_whole"))
)
