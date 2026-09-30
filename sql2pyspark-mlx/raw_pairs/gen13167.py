from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED"]))
    .select("status")
    .unionByName(df_shipments.select("status"))
    .distinct()
)
