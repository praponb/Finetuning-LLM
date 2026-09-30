from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "LOST") & F.col("carrier").isin(["FedEx", "Kerry"]))
    .withColumn("cost_int", F.col("cost").cast("int"))
)
