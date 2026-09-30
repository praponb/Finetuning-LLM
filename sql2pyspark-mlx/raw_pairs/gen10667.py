from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 5)
    .select("store_id")
    .unionByName(df_store_sales.filter(F.col("units") == 25).select("store_id"))
)
