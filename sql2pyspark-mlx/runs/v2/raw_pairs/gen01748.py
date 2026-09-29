from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") >= 0.29)
    .select("status")
    .unionByName(df_payments.filter(F.col("order_id").isNull()).select("status"))
    .distinct()
)
