from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 7, 8]))
    .select("id", "revenue", F.year("sale_date").alias("sale_date_year"))
)
