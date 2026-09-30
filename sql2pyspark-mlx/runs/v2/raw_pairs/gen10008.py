from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 4, 6]))
    .select("revenue", "sale_date")
    .orderBy("revenue")
)
