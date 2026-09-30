from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 9) & (F.col("store_id").isin([3, 4, 6]) | F.col("product_id").isin([1, 4, 6, 7])))
    .select("sale_date", "units")
    .orderBy("sale_date")
)
