from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "FedEx", "UPS"]))
    .groupBy(F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"))
    .agg(F.avg("cost").alias("avg_cost"))
    .orderBy(F.col("avg_cost").desc())
)
