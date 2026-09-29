from pyspark.sql import functions as F

result = (
    df_payments
    .select("id", "order_id", "status", "method")
    .orderBy(F.col("order_id").desc())
)
