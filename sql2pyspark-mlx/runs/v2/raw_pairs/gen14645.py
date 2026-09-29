from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "IN_TRANSIT", "RETURNED"]))
    .select("id", "delivered_date", F.upper("status").alias("status_upper"))
)
