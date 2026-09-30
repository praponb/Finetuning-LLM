from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", "carrier")
    .agg(
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
        F.countDistinct("cost").alias("unique_orders"),
    )
    .filter(F.col("delivered_cost") < 69)
    .select("status", "carrier", "delivered_cost", "unique_orders")
)
