from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Singapore", "New York"]) & F.col("sqft").between(4592, 11566))
    .select("id", "opened_date", "store_name", F.ceil("sqft").alias("sqft_whole"))
    .orderBy(F.col("sqft_whole").desc(), "id")
    .limit(5)
)
