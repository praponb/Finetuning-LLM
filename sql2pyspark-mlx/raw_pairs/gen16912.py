from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([5, 7, 15, 25]))
    .select("status")
    .unionByName(df_shipments.select("status"))
)
