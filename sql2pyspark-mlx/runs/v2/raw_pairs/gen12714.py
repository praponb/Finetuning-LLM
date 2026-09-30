from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("delivered_date") >= 2023)
    .groupBy(F.month("shipped_date").alias("shipped_month"))
    .agg(F.count("cost").alias("cost_count"))
)
