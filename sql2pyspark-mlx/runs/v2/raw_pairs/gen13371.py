from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .groupBy("status", "carrier")
    .agg(F.sum("cost").alias("total_cost"))
    .filter(F.col("total_cost") >= 480)
    .select("status", "carrier", "total_cost")
    .orderBy(F.col("total_cost").desc(), "carrier")
)
