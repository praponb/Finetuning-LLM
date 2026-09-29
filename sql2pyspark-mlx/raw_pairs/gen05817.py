from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount")
    .subtract(df_orders.filter(F.col("channel") == "web").select("shipping_fee"))
    .orderBy(F.col("amount").desc())
)
