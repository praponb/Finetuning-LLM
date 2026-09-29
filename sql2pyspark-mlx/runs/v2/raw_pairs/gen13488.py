from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter(F.col("carrier") == "UPS")

agg = (
    filtered_shipments
    .groupBy("status", F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", F.col("cost")).otherwise(0)).alias("kerry_cost"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
)

result = (
    agg
    .filter(F.col("kerry_cost") < 26)
    .select("status", "shipped_quarter", "kerry_cost", "latest_shipped_date")
)
