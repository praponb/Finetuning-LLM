from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select("city", "region")
    .unionByName(
        df_stores
        .filter(F.col("opened_date") >= "2024-06-12")
        .select("city", "region")
    )
    .orderBy("city")
)
