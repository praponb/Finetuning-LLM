from pyspark.sql import functions as F

result = (
    df_shipments
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .orderBy("cost_range")
)
