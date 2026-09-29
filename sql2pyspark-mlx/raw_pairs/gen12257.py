from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 34)
    .select("status")
    .subtract(df_payments.select("status"))
)
