from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "North") | (F.col("sqft") >= 4535))
    .select("id", "region", F.col("sqft").cast("double").alias("sqft_num"))
)
