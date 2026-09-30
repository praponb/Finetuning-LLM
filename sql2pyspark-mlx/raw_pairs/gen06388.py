from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(950, 1320) & (F.to_date("paid_at") < "2023-08-13") & (F.to_date("paid_at") >= "2023-06-01"))
    .select("order_id", "amount", F.round("amount", 1).alias("amount_rounded"))
)
