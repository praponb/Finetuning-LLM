from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select("id", "city")
    .unionByName(
        df_stores
        .filter(F.col("city").isin(["Berlin", "Chiang Mai", "Tokyo"]))
        .select("id", "city")
    )
    .distinct()
    .orderBy(F.col("id").desc())
)
