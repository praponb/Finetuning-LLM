from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.max("shipped_date").alias("last_shipped_date"),
        F.sum(F.when(F.col("carrier") == "DHL", 1).otherwise(0)).alias("dhl_count"),
    )
    .filter(F.col("dhl_count") > 46)
)
