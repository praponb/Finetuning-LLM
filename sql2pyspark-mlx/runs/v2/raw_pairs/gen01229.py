from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter(F.col("cost").between(49, 91))

agg = (
    filtered_shipments
    .groupBy("carrier")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)

result = (
    agg
    .filter(F.col("cost_range") > 37)
    .select("carrier", "cost_range")
    .orderBy(F.col("cost_range").desc())
)
