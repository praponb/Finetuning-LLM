from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(41, 53))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
        F.countDistinct("cost").alias("unique_orders"),
    )
)
