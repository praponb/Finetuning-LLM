from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 800)
    .select("order_id")
    .unionByName(df_payments.filter(F.col("paid_at") < "2024-05-01").select("order_id"))
)
