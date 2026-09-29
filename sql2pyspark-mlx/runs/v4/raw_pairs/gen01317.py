from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "Flash"]) & (F.month("delivered_date").isin([3, 7, 11]) | (F.col("delivered_date") < "2025-03-01")))
    .agg(F.max("cost").alias("max_cost"))
)
