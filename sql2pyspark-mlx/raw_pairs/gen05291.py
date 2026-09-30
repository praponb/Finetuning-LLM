from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "LOST"]))
    .select("cost", "carrier", "delivered_date")
)
