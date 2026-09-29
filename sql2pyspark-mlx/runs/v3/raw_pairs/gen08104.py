from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "Kerry", "FedEx"]) | (F.col("shipped_date") > "2023-12-01"))
    .groupBy("status", "carrier")
    .agg(
        F.max("shipped_date").alias("last_shipped_date"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("max_cost") < 59)
)
