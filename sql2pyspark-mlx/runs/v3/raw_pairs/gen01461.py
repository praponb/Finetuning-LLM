from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("delivered_date").alias("delivered_month"))
    .agg(F.max("cost").alias("max_cost"))
    .orderBy(F.col("delivered_month").desc())
    .limit(20)
)
