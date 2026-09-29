from pyspark.sql import functions as F

filtered_stores = df_stores.filter(F.upper("store_name").like("%LD %"))

summary = (
    filtered_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.max("opened_date").alias("last_opened_date"))
)

result = summary.select("opened_month", "last_opened_date")
