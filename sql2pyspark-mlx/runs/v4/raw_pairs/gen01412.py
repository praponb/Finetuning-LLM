from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2023-09-14")
    .select("status", F.col("cost").cast("int").alias("cost_int"))
)
