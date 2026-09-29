from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") < 89)
    .orderBy("order_id")
    .limit(5)
)
