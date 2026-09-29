from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 34)
    .select("status")
    .unionByName(df_payments.filter(F.col("amount") >= 860).select("status"))
    .distinct()
)
