from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2024-04-01", "2025-03-01") & (F.col("sqft") == 5368) & F.col("region").isin(["East", "South", "Central"]))
    .select("region", "store_name")
    .orderBy("store_name")
)
