from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .groupBy("status", "carrier")
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") > 85)
)
