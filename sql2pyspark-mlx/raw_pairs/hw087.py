from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "web")
    .select("id", "amount")
    .unionByName(df_payments.filter(F.col("status") == "FAILED").select("id", "amount"))
)
