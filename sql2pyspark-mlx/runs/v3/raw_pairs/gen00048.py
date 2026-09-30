from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 1300)
    .select("units")
    .unionByName(df_store_sales.filter(F.col("store_id") == 2).select("units"))
    .distinct()
)
