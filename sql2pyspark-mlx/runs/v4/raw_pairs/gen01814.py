from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("delivered_date") == 2024) & (F.year("delivered_date") == 2025))
    .withColumn("shipped_date_month_end", F.last_day("shipped_date"))
)
