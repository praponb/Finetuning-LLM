from pyspark.sql import functions as F

result = (
    df_payments
    .select("id", "order_id", F.substring("status", 1, 3).alias("status_prefix"))
    .orderBy("order_id", "id")
)
