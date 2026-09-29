from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", F.year("opened_date").alias("opened_year"))
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
    .orderBy(F.col("opened_year").desc())
)
