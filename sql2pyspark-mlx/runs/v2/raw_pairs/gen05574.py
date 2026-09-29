from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "IN_TRANSIT"]))
    .groupBy(F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"))
    .agg(F.count("cost").alias("cost_count"))
    .filter(F.col("cost_count") >= 4)
)
