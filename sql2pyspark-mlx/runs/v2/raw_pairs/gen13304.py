from pyspark.sql import functions as F

filtered_stores = df_stores.filter(F.col("opened_date") > "2023-08-01")

agg = (
    filtered_stores
    .groupBy("city")
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
)

result = (
    agg
    .filter(F.col("sqft_range") > 57)
    .orderBy(F.col("city").desc())
)
