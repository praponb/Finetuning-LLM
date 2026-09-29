from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2024-04-01", "2025-05-29"))
    .select("region", "sqft")
    .unionByName(df_stores.filter(F.col("sqft") <= 15441).select("region", "sqft"))
    .distinct()
    .orderBy("region")
)
