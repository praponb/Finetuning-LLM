from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]))
    .select("status")
    .unionByName(df_shipments.filter(F.col("carrier") != "UPS").select("status"))
)
