from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(10, 17))
    .select("id", "order_id", "status", "amount")
)
