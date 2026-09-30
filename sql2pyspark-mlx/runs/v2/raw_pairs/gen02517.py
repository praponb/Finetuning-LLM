from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2023-04-19")
    .select("store_name")
    .unionByName(
        df_stores
        .filter(F.col("opened_date") >= "2024-11-01")
        .select("store_name")
    )
    .orderBy("store_name")
)
