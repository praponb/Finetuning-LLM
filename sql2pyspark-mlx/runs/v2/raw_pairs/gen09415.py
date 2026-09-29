from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", "carrier")
    .agg(F.min("cost").alias("min_cost"))
    .filter(F.col("min_cost") < 54)
    .select("status", "carrier", "min_cost")
)
