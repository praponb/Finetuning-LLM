from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 1500)
    .select("product_id", "store_id", "units")
    .orderBy("store_id")
)
