from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("delivered_date").alias("delivered_month"))
    .agg(F.count("cost").alias("cost_count"))
    .orderBy(F.col("cost_count").desc(), "delivered_month")
)
