from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .select("status")
    .unionByName(df_shipments.select("status"))
    .distinct()
)
