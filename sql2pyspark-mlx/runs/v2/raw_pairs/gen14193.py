from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS"]))
    .select("status", "order_id")
    .orderBy("status")
)
