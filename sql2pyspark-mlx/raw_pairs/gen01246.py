from pyspark.sql import functions as F

result = (
    df_stores
    .select("region", "city")
    .exceptAll(
        df_stores
        .filter(F.col("store_name").like("%Cen%"))
        .select("region", "city")
    )
)
