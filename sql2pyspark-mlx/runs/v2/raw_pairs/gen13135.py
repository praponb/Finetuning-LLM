from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") >= "2024-10-01")
    .groupBy("carrier")
    .agg(F.min("cost").alias("min_cost"))
    .filter(F.col("min_cost") > 41)
    .select("carrier", "min_cost")
    .orderBy("min_cost", "carrier")
    .limit(1)
)
