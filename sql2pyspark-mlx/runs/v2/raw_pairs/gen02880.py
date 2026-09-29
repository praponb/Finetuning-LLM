from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") > 11040)
    .groupBy("region", F.year("opened_date").alias("opened_year"))
    .agg(F.min("opened_date").alias("min_opened_date"))
    .orderBy("min_opened_date")
)
