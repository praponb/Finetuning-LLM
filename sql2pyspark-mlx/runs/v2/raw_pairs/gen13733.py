from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "DELIVERED", "RETURNED"]))
    .select("cost", "delivered_date")
)
