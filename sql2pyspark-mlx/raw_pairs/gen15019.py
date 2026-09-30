from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["FedEx", "DHL"]) | F.col("carrier").isNotNull())
    .groupBy("carrier")
    .agg(F.sum("cost").alias("total_cost"))
)
