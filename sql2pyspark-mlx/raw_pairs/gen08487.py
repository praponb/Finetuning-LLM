from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "DHL", "FedEx"]) & (~F.col("status").isin(["DELIVERED", "RETURNED"]) | F.month("delivered_date").isin([2, 4, 10])))
    .groupBy("status")
    .agg(F.max("cost").alias("max_cost"))
    .orderBy(F.col("status").desc())
)
