from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select("status")
    .subtract(df_shipments.filter(F.col("cost") < 86).select("status"))
)
