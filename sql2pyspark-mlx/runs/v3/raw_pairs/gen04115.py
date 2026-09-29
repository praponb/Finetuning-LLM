from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3]))
    .select("revenue", "store_id", "product_id")
    .orderBy("store_id")
)
