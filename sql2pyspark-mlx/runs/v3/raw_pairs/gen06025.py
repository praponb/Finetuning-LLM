from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2025-08-01")
    .select("status")
    .unionByName(df_payments.filter(F.col("method") != "paypal").select("status"))
    .orderBy(F.col("status").desc())
)
