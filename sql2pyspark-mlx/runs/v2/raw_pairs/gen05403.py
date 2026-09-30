from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"), "city")
    .agg(F.min("opened_date").alias("min_opened_date"))
)
