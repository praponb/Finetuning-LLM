from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "Flash", "FedEx"]) & (F.col("carrier") != "FedEx"))
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"), "carrier")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.avg("cost").alias("avg_cost"),
    )
    .filter(F.col("avg_cost") < 54)
    .orderBy("cost_range", "shipped_quarter", "carrier")
    .limit(10)
)
