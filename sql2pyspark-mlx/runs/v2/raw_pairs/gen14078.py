from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "RETURNED"]))
    .select("carrier", "status")
    .distinct()
)
