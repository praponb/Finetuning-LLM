from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("delivered_month"))
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") < 29)
)
