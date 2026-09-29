from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([2, 7, 11]) & (F.col("product_id") == 6) & (F.col("units") >= 30))
    .select("id", "product_id")
    .orderBy("product_id", F.col("id").desc())
)
