from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2023-09-01") & F.col("amount").between(960, 1130))
)
