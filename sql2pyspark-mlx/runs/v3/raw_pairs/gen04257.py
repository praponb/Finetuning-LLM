from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "Flash", "FedEx"]))
    .groupBy("carrier")
    .agg(F.sum(F.when(F.col("carrier") == "DHL", 1).otherwise(0)).alias("dhl_count"))
    .orderBy("carrier")
    .limit(5)
)
