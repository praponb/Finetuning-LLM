from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(df_payments.filter(F.col("amount") >= 1310).select("status"))
)
