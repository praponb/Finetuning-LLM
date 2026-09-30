from pyspark.sql import functions as F

result = (
    df_payments
    .select("id", "amount", "status")
    .orderBy("status", "id")
    .limit(5)
)
