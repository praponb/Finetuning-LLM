from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .select("method")
    .subtract(df_payments.filter(F.col("order_id").isNotNull()).select("method"))
)
