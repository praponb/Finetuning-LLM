from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 1110) & ~F.col("method").isin(["card", "promptpay"]))
    .select("amount", "status", "method")
)
