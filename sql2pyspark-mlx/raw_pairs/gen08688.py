from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 2) & F.col("method").isin(["cash", "bank_transfer", "card"]))
    .withColumn("status_title", F.initcap(F.trim("status")))
    .withColumn("amount_int", F.col("amount").cast("int"))
)
