from pyspark.sql import functions as F

result = (
    df_payments
    .select("order_id")
    .orderBy("order_id")
)
