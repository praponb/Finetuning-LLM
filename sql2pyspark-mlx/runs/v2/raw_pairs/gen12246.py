from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
    .select("order_id")
    .intersect(df_payments.filter(F.col("paid_at") < "2023-03-01").select("order_id"))
)
