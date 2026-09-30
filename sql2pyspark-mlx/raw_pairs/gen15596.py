from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") != 9621)
    .select("sqft", "city", "opened_date", F.floor("sqft").alias("sqft_whole"))
    .orderBy("opened_date")
)
