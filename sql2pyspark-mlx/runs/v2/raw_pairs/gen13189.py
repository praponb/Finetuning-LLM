from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", "region")
    .agg(F.min("opened_date").alias("min_opened_date"))
)
