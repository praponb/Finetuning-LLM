from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") > 11010)
    .select("id", "region")
    .unionByName(
        df_stores
        .filter(F.lower("store_name").like("%own%"))
        .select("id", "region")
    )
    .distinct()
)
