from pyspark.sql import functions as F

result = (
    df_payments
    .select("method", "amount")
    .orderBy("amount")
)
