from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "card") & ((F.col("amount") < 750) | (F.col("method") != "promptpay")))
    .select("paid_at")
    .orderBy(F.col("paid_at").desc())
)
