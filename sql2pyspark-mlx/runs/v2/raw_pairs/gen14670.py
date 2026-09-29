from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") >= "2025-05-30")
    .select("method")
    .subtract(df_payments.filter(F.col("paid_at") < "2023-10-13").select("method"))
    .orderBy("method")
)
