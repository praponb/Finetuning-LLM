from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & (F.col("order_id") == 27) & (F.col("amount") <= 970))
    .orderBy("status", "id")
    .limit(3)
)
