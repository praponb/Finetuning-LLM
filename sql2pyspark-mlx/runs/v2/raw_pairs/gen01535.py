from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.max("opened_date").alias("last_opened_date"))
    .orderBy("opened_month", "last_opened_date")
)
