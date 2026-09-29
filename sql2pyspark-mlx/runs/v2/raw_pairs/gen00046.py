from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["RETURNED", "IN_TRANSIT", "DELIVERED"]))
    .withColumn("cost_whole", F.floor("cost"))
)
