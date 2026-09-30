from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("delivered_month"), "status")
    .agg(F.sum("cost").alias("total_cost"))
    .filter(F.col("total_cost") >= 570)
    .orderBy("delivered_month", "status")
    .limit(5)
)
