from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"))
    .agg(F.count("*").alias("num_shipments"))
    .filter(F.col("num_shipments") >= 8)
)
