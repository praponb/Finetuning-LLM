from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.count(F.when(F.col("region") == "North", 1)).alias("north_count"))
    .filter(F.col("north_count") >= 10)
    .select("opened_month", "north_count")
    .orderBy(F.col("north_count").desc(), "opened_month")
    .limit(50)
)
