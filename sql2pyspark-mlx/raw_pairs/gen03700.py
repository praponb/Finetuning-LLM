from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "Kerry", "DHL"]))
    .groupBy("carrier", F.quarter("shipped_date").alias("shipped_quarter"))
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .filter(F.col("cost_range") >= 27)
    .orderBy(F.col("shipped_quarter").desc())
)
