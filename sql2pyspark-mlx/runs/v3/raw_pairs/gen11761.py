from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.min("opened_date").alias("first_opened_date"))
    .orderBy("first_opened_date", F.col("city").desc())
)
