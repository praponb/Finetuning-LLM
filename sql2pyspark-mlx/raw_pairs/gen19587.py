from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") >= "2023-12-01")
    .orderBy("amount", "id")
    .limit(3)
)
