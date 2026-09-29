from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Siam%"))
    .select("region")
    .unionByName(
        df_stores
        .filter(F.col("store_name").like("%Riverside%"))
        .select("region")
    )
    .orderBy("region")
)
