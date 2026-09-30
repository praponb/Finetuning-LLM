from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(~((F.col("status") == "RETURNED") & F.col("carrier").isin(["Kerry", "FedEx", "DHL"])))
    .withColumn("delivered_date_label", F.date_format("delivered_date", "yyyy-MM"))
)

agg = (
    recent_shipments
    .groupBy("status")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)

result = (
    agg
    .filter(F.col("cost_range") > 68)
    .select("status", "cost_range")
    .orderBy(F.col("cost_range").desc(), F.col("status").desc())
)
