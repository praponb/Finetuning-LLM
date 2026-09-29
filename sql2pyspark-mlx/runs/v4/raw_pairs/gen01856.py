from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .filter(F.col("cost_range") >= 27)
)
