from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") != "SUCCESS") | F.col("order_id").isNull())
    .select("order_id", F.col("amount").cast("int").alias("amount_int"))
)
