from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.35)
    .select("status")
    .unionByName(df_payments.filter(F.col("amount") >= 700).select("status"))
)
