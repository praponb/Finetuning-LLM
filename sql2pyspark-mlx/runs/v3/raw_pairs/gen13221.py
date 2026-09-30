from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(9, 16))
    .select("paid_at", "amount", "order_id")
)
