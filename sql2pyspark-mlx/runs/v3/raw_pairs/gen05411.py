from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "IN_TRANSIT"]))
    .withColumn("cost_int", F.col("cost").cast("int"))
)
