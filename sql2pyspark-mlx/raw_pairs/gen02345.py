from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") != 8126) & (F.col("region") == "Central") & (F.col("region") != "South"))
    .select("id", "store_name", "region", "opened_date")
)
