from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 740) & (F.col("paid_at") < "2023-04-01"))
    .select("id", "amount")
)
