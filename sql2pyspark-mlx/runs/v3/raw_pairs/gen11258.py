from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("delivered_date").alias("delivered_month"), "carrier")
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") > 65)
)
