from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") >= "2023-09-01")
    .select("id", "amount", "status")
    .orderBy("status")
)
