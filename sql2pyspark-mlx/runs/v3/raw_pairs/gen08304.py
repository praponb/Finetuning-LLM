from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") != "SUCCESS") & (F.col("method") == "promptpay"))
    .select("method")
)
