from pyspark.sql import functions as F

result = (
    df_payments
    .select("method")
    .intersect(df_shipments.filter(F.col("order_id").isNull()).select("status"))
    .orderBy("method")
)
