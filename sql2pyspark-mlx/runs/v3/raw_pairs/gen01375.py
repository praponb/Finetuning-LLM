from pyspark.sql import functions as F

result = (
    df_stores
    .select("city", "region", "opened_date", F.floor("sqft").alias("sqft_whole"))
)
