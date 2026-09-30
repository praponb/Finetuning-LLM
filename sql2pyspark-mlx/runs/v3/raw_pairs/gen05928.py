from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Chiang Mai") & (F.col("opened_date") < "2023-08-01"))
    .select("region", "store_name", "sqft")
)
