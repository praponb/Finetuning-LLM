from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") != "Flash")
    .groupBy("carrier", "status")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .filter(F.col("cost_range") < 25)
    .select("carrier", "status", "cost_range")
    .orderBy("carrier")
)
