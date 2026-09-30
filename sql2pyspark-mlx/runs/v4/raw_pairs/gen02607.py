from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.length("store_name") < 10) | F.month("opened_date").isin([4, 7, 8]))
    .withColumn("region_lower", F.lower("region"))
)
