from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(7, 11))
    .select("status")
    .intersect(df_subscriptions.select("status"))
)
