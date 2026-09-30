from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 23)
    .select("status")
    .unionByName(df_payments.select("status"))
)
