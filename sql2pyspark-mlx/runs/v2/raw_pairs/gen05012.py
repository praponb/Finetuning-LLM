from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(df_payments.filter(F.col("paid_at") >= "2024-10-26").select("status"))
)
