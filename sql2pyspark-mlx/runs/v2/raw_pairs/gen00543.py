from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2023-07-01")
    .select("status")
    .distinct()
    .orderBy("status")
)
