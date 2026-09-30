from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2023-06-01")
    .select("id", "status")
    .unionByName(df_payments.filter(F.col("amount") <= 420).select("id", "status"))
    .distinct()
)
