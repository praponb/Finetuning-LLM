from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(11, 18))
    .select("status")
    .unionByName(df_shipments.select("status"))
)
