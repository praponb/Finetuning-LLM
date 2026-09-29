from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "web")
    .select("status")
    .unionByName(df_payments.select("status"))
    .distinct()
)
