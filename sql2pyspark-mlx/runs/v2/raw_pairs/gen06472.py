from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"))
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .filter(F.col("cost_range") < 45)
)
