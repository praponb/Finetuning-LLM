from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "DELIVERED", "RETURNED"]) & F.col("carrier").isin(["DHL", "FedEx"]) & F.col("carrier").isin(["Kerry", "FedEx", "DHL"]))
)

summary = (
    base
    .groupBy(F.year("shipped_date").alias("shipped_year"), "carrier")
    .agg(F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"))
)

result = summary.orderBy(F.col("shipped_year").desc())
