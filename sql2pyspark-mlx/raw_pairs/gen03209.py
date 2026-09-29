from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2025-08-01")
    .select("region", "city")
    .subtract(df_stores.filter(F.col("sqft") <= 11668).select("region", "city"))
)
