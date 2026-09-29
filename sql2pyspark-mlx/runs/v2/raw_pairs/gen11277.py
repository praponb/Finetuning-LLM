from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(F.max("opened_date").alias("last_opened_date"))
    .orderBy("region")
)
