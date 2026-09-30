from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select("city")
    .unionByName(
        df_stores
        .filter(F.col("city").isin(["Singapore", "Chiang Mai", "New York"]))
        .select("city")
    )
)
