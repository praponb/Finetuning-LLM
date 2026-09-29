from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["DHL", "UPS"]))
    .groupBy("status")
    .agg(
        F.count("cost").alias("cost_count"),
        F.min("shipped_date").alias("min_shipped_date"),
    )
)
