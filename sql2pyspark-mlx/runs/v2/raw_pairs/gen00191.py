from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("carrier") == "UPS", F.col("cost")).otherwise(0)).alias("ups_cost"),
        F.max("delivered_date").alias("latest_delivered_date"),
    )
    .filter(F.col("ups_cost") >= 69)
    .select("status", "ups_cost", "latest_delivered_date")
)
