from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 35)
    .select("status")
    .unionByName(df_shipments.filter(F.col("cost") < 32).select("status"))
    .orderBy(F.col("status").desc())
)
