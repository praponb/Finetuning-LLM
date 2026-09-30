from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "West") & ((F.col("region") == "West") | (F.col("sqft") <= 14259)))
    .select("opened_date")
)
