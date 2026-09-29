from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2025-10-01")
    .select("order_id")
    .unionByName(df_payments.filter(F.col("amount").isNotNull()).select("order_id"))
    .orderBy("order_id")
)
