from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 730) & F.col("amount").between(730, 1180) & (F.col("paid_at") < "2024-07-09"))
    .select("amount", F.substring("status", 1, 4).alias("status_prefix"))
)
