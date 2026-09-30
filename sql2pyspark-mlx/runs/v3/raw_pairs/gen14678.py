from pyspark.sql import functions as F

result = (
    df_payments
    .select("amount")
    .subtract(df_orders.select("shipping_fee"))
)
