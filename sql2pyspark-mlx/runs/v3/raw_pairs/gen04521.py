from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") <= 6540)
    .select("city", "store_name")
    .unionByName(
        df_stores
        .filter(F.col("city") == "Chiang Mai")
        .select("city", "store_name")
    )
    .orderBy(F.col("city").desc())
)
