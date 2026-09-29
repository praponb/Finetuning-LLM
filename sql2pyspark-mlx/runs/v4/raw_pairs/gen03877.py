from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") > 40) & F.col("carrier").isin(["Kerry", "FedEx"]))
    .groupBy("status", "order_id", "carrier")
    .agg(F.count("*").alias("row_count"))
    .orderBy("status")
)
