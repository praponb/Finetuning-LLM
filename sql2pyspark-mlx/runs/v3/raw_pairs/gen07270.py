from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .groupBy("carrier")
    .agg(F.min("cost").alias("min_cost"))
)
