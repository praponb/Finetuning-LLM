from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 10801)
    .select("id", "store_name", F.col("sqft").cast("double").alias("sqft_num"))
    .orderBy(F.col("id").desc(), "sqft_num")
)
