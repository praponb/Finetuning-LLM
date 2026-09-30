from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "SHIPPED")
    .select("status")
    .intersect(df_payments.filter(F.hour("paid_at").between(9, 12)).select("status"))
)
