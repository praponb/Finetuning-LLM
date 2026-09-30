from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select("store_name", "city")
    .intersect(
        df_stores
        .filter(F.col("region").isin(["Central", "West", "East"]))
        .select("store_name", "city")
    )
    .orderBy("store_name")
)
