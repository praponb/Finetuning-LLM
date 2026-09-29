from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("status")
    .unionByName(df_payments.filter(F.col("paid_at") >= "2024-01-01").select("status"))
    .distinct()
)
