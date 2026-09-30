from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 28)
    .select("method", "status")
    .distinct()
)
