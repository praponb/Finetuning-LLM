from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") < "2025-04-20")
    .select("city", "region")
    .unionByName(df_stores.filter(F.col("sqft") != 9031).select("city", "region"))
)
