from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") >= "2024-12-01") & F.col("store_name").like("%Downtown%"))
    .select("sqft", "region", F.dayofweek("opened_date").alias("opened_date_dayofweek"))
)
