from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"))
    .filter(F.col("new_york_count") > 3)
    .select("opened_quarter", "new_york_count")
    .orderBy(F.col("opened_quarter").desc(), F.col("new_york_count").desc())
)
