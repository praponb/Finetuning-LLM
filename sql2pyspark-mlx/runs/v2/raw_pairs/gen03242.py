from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "DHL", "FedEx"]) | (F.year("shipped_date") == 2023))
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .orderBy("status", F.col("dhl_count").desc())
)
