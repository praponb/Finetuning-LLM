from pyspark.sql import functions as F

result = (
    df_payments
    .select("id", "status", "order_id", F.to_date("paid_at").alias("paid_at_date"))
)
