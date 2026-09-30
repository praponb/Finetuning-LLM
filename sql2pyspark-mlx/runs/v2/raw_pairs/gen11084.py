from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "cash"]))
    .select("status")
    .intersect(df_shipments.select("status"))
)
