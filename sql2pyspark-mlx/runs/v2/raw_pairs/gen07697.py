from pyspark.sql import functions as F

base = df_stores.filter(F.col("store_name").like("Air%") | (F.year("opened_date") >= 2025))

agg = (
    base
    .groupBy("city")
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
)

result = agg.orderBy(F.col("min_sqft").desc(), "city")
