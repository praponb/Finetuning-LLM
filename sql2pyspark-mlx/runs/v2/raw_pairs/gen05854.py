from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") != "Flash")
    .groupBy("carrier", "status")
    .agg(F.countDistinct("cost").alias("unique_statuss"))
)
