from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "North")
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.max("opened_date").alias("last_opened_date"))
    .orderBy("opened_month")
)
