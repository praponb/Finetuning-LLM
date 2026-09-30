from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter(~((F.col("cost") <= 64) | F.col("carrier").isin(["DHL", "Kerry", "FedEx"])))
)

agg = (
    valid_shipments
    .groupBy("status", F.date_trunc("MONTH", "shipped_date").alias("shipped_month"))
    .agg(
        F.count("cost").alias("cost_count"),
        F.max("delivered_date").alias("last_delivered_date"),
    )
)

result = (
    agg
    .select("status", "shipped_month", "last_delivered_date")
    .orderBy(F.col("status").desc(), "shipped_month")
    .limit(50)
)
