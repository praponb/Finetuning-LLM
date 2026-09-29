from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 14)
    .select("status")
    .intersect(df_shipments.select("status"))
)
