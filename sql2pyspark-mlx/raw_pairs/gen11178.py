from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(F.count("*").alias("num_shipments"))
    .orderBy("carrier")
)
