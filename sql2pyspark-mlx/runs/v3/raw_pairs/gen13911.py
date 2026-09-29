from pyspark.sql import functions as F

src = df_stores.filter(F.col("region").isNull())

agg = (
    src
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"))
)

result = agg.select("opened_month", "berlin_count")
