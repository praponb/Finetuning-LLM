from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id") == 6)
    .select("store_id", "units")
    .unionByName(
        df_store_sales
        .filter(F.col("revenue") <= 1700)
        .select("store_id", "units")
    )
    .orderBy("units")
)
