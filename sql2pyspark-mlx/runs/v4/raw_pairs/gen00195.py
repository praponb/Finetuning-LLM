from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.year("opened_date") == 2025)

summary = (
    valid_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "region")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.count("*").alias("cnt"),
    )
)

result = summary
