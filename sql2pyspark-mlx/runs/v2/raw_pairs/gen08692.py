from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-06-01")
    .select("id", "method")
    .unionByName(df_payments.filter(F.col("amount").isNotNull()).select("id", "method"))
)
